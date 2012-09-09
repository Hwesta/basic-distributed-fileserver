#! /usr/bin/python2
# TODO Remember to change this to python
# -*- test-case-name: a2.test.test_server -*-

# Twisted - networking library
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.protocols.policies import TimeoutMixin
from twisted.internet import reactor

# Other
import argparse
import os
import sys
import errno
import time
import re
import shelve  # For writing out dictionary
import shutil  # For file copy
import hashlib

verbosity = 0

def parse_args():
    parser = argparse.ArgumentParser(description='Run a replicated fileserver.')
    parser.add_argument('-ip', default='127.0.0.1',
                        help="Server IP.  Defaults to 127.0.0.1")
    parser.add_argument('-port', default=8080, type=int,
                        help="Server port.  Defaults to 8080")
    parser.add_argument('-dir', required=True,
                        help='Directory to store files in.  Required.')
    parser.add_argument('-primary','-p', required=True,
                        help='Absolute path to primary.txt file.  Required.')
    parser.add_argument("-v", "--verbosity", action="count", help="Show debugging output.")
    args = parser.parse_args()

    global verbosity
    verbosity = args.verbosity
    return args


# Messages parsing modeled off of twisted.web.http HTTPClient and HTTPChannel
class FilesystemProtocol(LineReceiver, TimeoutMixin):
    """ Reads and parses a message for the distributed filesystem. """
    method = None
    txn = None
    seq = None
    length = None
    buf = None
    data = True  # Is there data for this message?
    firstLine = True

    def connectionMade(self):
        self.setTimeout(3)  # seconds

    def connectionLost(self, reason):
        self.setTimeout(None)

    def lineReceived(self, line):
        self.resetTimeout()
        if self.firstLine:
            self.firstLine = False
            l = line.split()
            if len(l) != 4:
                self.sendError(204, "Header has the wrong number of fields.")
                return
            self.method, self.txn, self.seq, self.length = l

            try:
                self.txn = int(self.txn)
                self.seq = int(self.seq)
                self.length = int(self.length)
            except ValueError:
                self.sendError(204, "Header has non-numeric value.")
                return

            if self.seq < 0:
                self.sendError(204,
                               "Sequence number has to be a positive integer.")
                return
            return
        # No expected data, so process the message
        if not self.data:
            self.processMessage()
        # Blank line - prepare to process data
        if not line:
            if self.length == 0:  # Probably COMMIT or ABORT
                self.data = False
                return
            self.buf = ""
            self.setRawMode()  # Data arrives at rawDataReceived

    def rawDataReceived(self, data):
        if self.length is not None:
            data = data[:self.length]
            self.buf += data
            self.length -= len(data)
        if self.length == 0:
            self.processMessage()

    def timeoutConnection(self):
        print "Timing out client: %s" % str(self.transport.getPeer())
        self.sendError(204,
                       "Connection timed out (is length longer than data?)")

    def sendError(self, err_num, err_reason):
        if not isinstance(self.txn, int):
            self.txn = -1
        error = "ERROR %d 0 %d %d\r\n\r\n%s\r\n\r\n" % (
                self.txn, err_num, len(err_reason), err_reason)
        self.transport.write(error)
        self.transport.loseConnection()

    def sendACK(self):
        ack = "ACK %d 0 0 0\r\n\r\n\r\n" % (self.txn)
        self.transport.write(ack)
        self.transport.loseConnection()

    def sendASK_RESEND(self, missing_writes):
        resend_string = "ASK_RESEND %d %d 0 0\r\n\r\n\r\n"
        for write in missing_writes:
            resend = resend_string % (self.txn, write)
            self.transport.write(resend)
        self.transport.loseConnection()

    def processMessage(self):
        self.setTimeout(None)
        if self.method == "READ":
            self.processREAD()
        elif self.method == "NEW_TXN":
            self.processNEW_TXN()
        elif self.method == "WRITE":
            self.processWRITE()
        elif self.method == "COMMIT":
            self.processCOMMIT()
        elif self.method == "ABORT":
            self.processABORT()
        else:
            self.sendError(204, "Method does not exist.")

    def processREAD(self):
        (error, buf) = self.factory.readFile(self.buf)
        if error != 0:
            self.sendError(error, buf)
        else:
            self.transport.write(buf)
            self.transport.loseConnection()

    def processNEW_TXN(self):
        (txn_id, error, error_reason) = self.factory.startNewTxn(self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        else:
            self.txn = txn_id
            self.sendACK()

    def processWRITE(self):
        (error, error_reason) = self.factory.saveWrite(self.txn, self.seq, self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        self.transport.loseConnection()

    def processABORT(self):
        (error, error_reason) = self.factory.abortTxn(self.txn)
        if error != 0:
            self.sendError(error, error_reason)
        else:
            self.sendACK()

    def processCOMMIT(self):
        (decision, error, details) = self.factory.commitTxn(self.txn, self.seq)
        if decision == 'ACK':
            self.sendACK()
        elif decision == 'ERROR':
            self.sendError(error, details)
        elif decision == 'ASK_RESEND':
            self.sendASK_RESEND(details)


class FilesystemFactory(Factory):
    """ Acts on parsed messages for distributed filesystem. """
    protocol = FilesystemProtocol
    logdir = ".server_log/"
    logfile = self.logdir+"log"
    lock_prefix = ".lock-"
    txn_list = None
    file_list = {}
    primary = None
    role = None
    ip = None
    port = None

    # Run on server startup
    def __init__(self,args):
        # Change working directory, if it exists
        if not os.path.isdir(args.dir):
            print "Path %s does not exist or is not a directory." % args.dir
            sys.exit(-1)
        else:
            os.chdir(args.dir)
        if verbosity > 0:
            print "Working directory: ", args.dir

        # Set path to primary.txt, if not at directory
        if os.path.isdir(args.primary):
            print "Primary.txt must point to a file, not a directory."
            sys.exit(-1)
        else:
            self.primary = args.primary

        self.ip = args.ip
        self.port = args.port

    def startFactory(self):
        # Create log directory
        try:
            os.makedirs(self.logdir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                print "Could not initialize server.  Does the directory have execute permission?"
                sys.exit(-1)

        # Delete lock files leftover from a crash
        pattern = "^"+self.lock_prefix+".*$"
        for f in os.listdir(self.logdir):
            if re.search(pattern, f):
                os.remove(os.path.join(self.logdir, f))

        # If logs exist, read from disk
        if os.path.isfile(self.logfile):
            log = shelve.open(self.logfile)
        else:
            log = shelve.open(self.logfile)
            log['next_id'] = 1
        self.txn_list = log
        if verbosity > 1:
            print "Raw log:", self.txn_list

        self.hashFiles()

        # Read primary.txt
        try:
            f = open(self.primary)
        except:
            print "Primary.txt could not be opened."
            sys.exit(-1)

        # Determine role from primary.txt
        l = f.readline()
        try:
            ip, port = l.split()
            if ip == self.ip and port == str(self.port):
                self.becomePrimary()
            # elif ip == 'localhost':
            #     print "Cannot run on localhost.  Exiting."
            #     sys.exit(-1)
            else:
                self.becomeSecondary()
        except ValueError:
            self.becomePrimary()
        if verbosity > 0:
            print "Role:", self.role


    # Run on server shutdown
    def stopFactory(self):
        # Abort transactions that were started more than 5 mins ago
        if self.txn_list is not None:
            expire_time = 5*60
            for (txn_id, txn) in self.txn_list.items():
                if (txn_id != 'next_id' and
                    txn['status'] == 'NEW_TXN' and
                    time.time() > txn['start_time'] + expire_time):

                    txn_info = self.txn_list[str(txn_id)]
                    txn_info['status'] = 'ABORT'
                    self.txn_list[str(txn_id)] = txn_info
            self.txn_list.close()

    def hashFiles(self):
        """  Create and hash files known by the server. """
        hashs = [(f, hashlib.md5(open(f, 'r').read()).hexdigest()) for f in os.listdir('.') if not os.path.isdir(f) and f[0] != '.']
        self.file_list = dict(hashs)
        if verbosity > 1:
            print "Hashes:", self.file_list


    def becomePrimary(self):
        """ Switch role to primary server. """
        self.role = 'PRIMARY'

    def becomeSecondary(self):
        """ Set up secondary server. """
        self.role = 'SECONDARY'

        # Remove NEW_TXNs if secondary
        # TODO check when need to forget NEW_TXNs
        for (txn_id, txn) in self.txn_list.items():
            if (txn_id != 'next_id' and txn['status'] == 'NEW_TXN'):
                del self.txn_list[str(txn_id)]
        if verbosity > 0:
            print 'Cleaned Log:', self.txn_list

        # If secondary, sync with primary

    def readFile(self, file_name):
        if self.role == 'SECONDARY':
            return (207, "This is the secondary.  Contact primary server.")
        if not os.path.isfile(file_name):
            return (206, "File not found.")

        try:
            f = open(file_name, 'r')
        except:
            return (205, "Unable to open file.  Check server settings.")

        try:
            buf = f.read(102400)  # Only read up to 100KB
        except:
            return (205,
                    "File IO error.  Check server settings and permissions.")
        finally:
            f.close()

        return (0, buf)

    def startNewTxn(self, new_file):
        # Error checking
        if self.role == 'SECONDARY':
            return (0, 207, "This is the secondary.  Contact primary server.")
        if os.path.isdir(new_file):
            return (0, 205, "A directory with that name already exists.")
        if new_file[0] == '.':
            return (0, 202, "Creating hidden files is forbidden.")
        if '/' in new_file:
            return (0, 202, "Creating directories (or files in subdirectories) is forbidden.")

        # New transaction information
        txn_id = self.txn_list['next_id']
        txn_info = {'file': new_file,
                    'status': 'NEW_TXN',
                    'writes': {},
                    'writes_committed': -1,
                    'start_time': time.time()}

        # Update log
        self.txn_list['next_id'] = txn_id + 1
        self.txn_list[str(txn_id)] = txn_info

        # Flush log to disk
        self.txn_list.sync()
        self.txn_list.sync()

        print 'Log:', self.txn_list
        return (txn_id, 0, None)

    def saveWrite(self, txn_id, seq, buf):
        # Error checking
        if self.role == 'SECONDARY':
            return (207, "This is the secondary.  Contact primary server.")
        if str(txn_id) not in self.txn_list:
            return (201, "Unknown transaction id.")

        txn_info = self.txn_list[str(txn_id)]
        if txn_info['status'] == 'ABORT':
            return (202, "Transaction has been aborted.")
        elif txn_info['status'] == 'COMMIT':
            return (202, "Transaction has been comitted already.")

        # Write to log
        txn_info['writes'][seq] = buf
        self.txn_list[str(txn_id)] = txn_info
        self.txn_list.sync()

        print 'Log:', self.txn_list
        return (0, None)

    def abortTxn(self, txn_id):
        # Error checking
        if self.role == 'SECONDARY':
            return (207, "This is the secondary.  Contact primary server.")
        if str(txn_id) not in self.txn_list:
            return (201, "Unknown transaction id.")
        txn_info = self.txn_list[str(txn_id)]

        if txn_info['status'] == 'COMMIT':
            return (202, "Transaction has been comitted already.")

        # Update status
        txn_info['status'] = 'ABORT'

        # Write to log, write out
        self.txn_list[str(txn_id)] = txn_info
        self.txn_list.sync()

        print 'Log:', self.txn_list
        return (0, None)

    def commitTxn(self, txn_id, seq):
        # Error checking
        if self.role == 'SECONDARY':
            return ('ERROR', 207, "This is the secondary.  Contact primary server.")
        if str(txn_id) not in self.txn_list:
            return ('ERROR', 201, "Unknown transaction id.")
        txn_info = self.txn_list[str(txn_id)]

        if txn_info['status'] == 'ABORT':
            return ('ERROR', 202, "Transaction has been aborted already.")
        elif txn_info['status'] == 'COMMIT':  # Don't recheck sequence number
            return ('ACK', 0, None)

        # Check that have right number of writes
        unsent = [k for k in range(0,seq) if k not in txn_info['writes']]
        if len(unsent) != 0:
            return ('ASK_RESEND', 0, unsent)

        # Write to the file

        filename = txn_info['file']
        lock_file = self.logdir + self.lock_prefix + filename  # Make this a hash??
        data = "".join(
            [txn_info['writes'][k] for k in txn_info['writes'] if k < seq])

        # If the lock file exists, another transaction is comitting
        while os.path.isfile(lock_file):
            print 'locked', txn_id
            time.sleep(0.05)

        try:
            # Copy existing file to lock
            if os.path.isfile(filename):
                shutil.copy2(filename, lock_file)
            # Write data
            f = open(lock_file, 'a')
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            txn_info['status'] = 'COMMIT'
            txn_info['writes_committed'] = seq-1

            # Copy back
            shutil.move(lock_file, filename)
        except:
            return ('ERROR', 205, 
                    "File IO error.  Check server settings and permissions.")
        finally:
            try:
                f.close()
            except:
                pass
            try:
                os.remove(lock_file)
            except:
                pass

        # Write to log, write out
        self.txn_list[str(txn_id)] = txn_info
        self.txn_list.sync()

        print 'Log:', self.txn_list
        return ('ACK', 0, None)


def main():
    args = parse_args()

    factory = FilesystemFactory(args)

    reactor.listenTCP(args.port, factory, interface=args.ip)
    reactor.run()

# Start the server
if __name__ == '__main__':
    main()
