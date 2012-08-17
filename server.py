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
import shelve  # For writing out dictionary
import shutil  # For file copy


# Messages parsing modeled off of twisted.web.http HTTPClient and HTTPChannel
class Server(LineReceiver, TimeoutMixin):
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
                self.sendError(204, "Header is wrong length")
                return
            self.method, self.txn, self.seq, self.length = l
            try:
                self.txn = int(self.txn)
                self.seq = int(self.seq)
                self.length = int(self.length)
            except ValueError:
                self.sendError(204, "Header has non-numeric value")
                return

            if seq < 0:
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
            self.sendError(204, "Method does not exist")

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


class ServerFactory(Factory):
    """ Acts on parsed messages for distributed filesystem. """
    protocol = Server
    logdir = None
    logfile = None
    txn_list = None

    def __init__(self, cwd):
        # Check if directory exists
        if not os.path.isdir(cwd):
            print "Path %s does not exist or is not a directory." % cwd
            sys.exit(-1)
        else:
            os.chdir(cwd)
        print "Working directory: ", cwd

        # Create hidden log dir
        self.logdir = ".server_log/"
        try:
            os.makedirs(self.logdir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                print "Could not initialize server.  Does the directory have execute permission?"
                sys.exit(-1)

        # If logs exist, read from disk
        self.logfile = self.logdir + "log"
        if os.path.isfile(self.logfile):
            log = shelve.open(self.logfile)
        else:
            log = shelve.open(self.logfile)
            log['next_id'] = 1
        self.txn_list = log
        print 'Log:', self.txn_list

    def __del__(self):
        if self.txn_list is not None:
            self.txn_list.close()

    def readFile(self, file_name):
        if not os.path.isfile(file_name):
            return (206, "File not found")

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
        txn_id = self.txn_list['next_id']
        txn_info = {'file': new_file, 'status': 'NEW_TXN', 'writes': {}}

        # Directory with the same name - error
        if os.path.isdir(new_file):
            return (txn_id, 205, "A directory with that name already exists.")

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
        if str(txn_id) not in self.txn_list:
            return ('ERROR', 201, "Unknown transaction id.")
        txn_info = self.txn_list[str(txn_id)]

        if txn_info['status'] == 'ABORT':
            return ('ERROR', 202, "Transaction has been aborted already.")
        elif txn_info['status'] == 'COMMIT':  # Don't recheck sequence number
            return ('ACK', 0, None)

        # Check that have right number of writes
        unsent = [k for k in range(seq) if k not in txn_info['writes']]
        if len(unsent) != 0:
            return ('ASK_RESEND', 0, unsent)

        # Write to the file

        filename = txn_info['file']
        lock_file = self.logdir + ".lock-" + filename  # Make this a hash??
        data = "".join(
            [txn_info['writes'][k] for k in txn_info['writes'] if k < seq])

        # If the lock file exists, another transaction is comitting
        while os.path.isfile(lock_file):
            print 'locked'
            sleep(0.05)

        try:
            # Copy existing file to lock
            if os.path.isfile(filename):
                shutil.copy2(new_file, lock_file)
            # Write data
            f = open(lock_file, 'a')
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            txn_info['status'] = 'COMMIT'

            # Copy back
            shutil.move(lock_file, filename)
        except:
            return ('ERROR', 205,
                    "File IO error.  Check server settings and permissions.")
        finally:
            f.close()
            try:
                os.remove(lock_file)
            except:
                pass

        # Write to log, write out
        self.txn_list[str(txn_id)] = txn_info
        self.txn_list.sync()

        print 'Log:', self.txn_list
        return ('ACK', 0, None)


def runserver():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run a distributed fileserver.')
    parser.add_argument('-ip', default='127.0.0.1', help="Server IP.")
    parser.add_argument('-port', default=8080, type=int, help="Server port")
    parser.add_argument('-dir', required=True, help='Directory to store files in.')
    args = parser.parse_args()

    reactor.listenTCP(args.port, ServerFactory(args.dir), interface=args.ip)
    reactor.run()

# Start the server
if __name__ == '__main__':
    runserver()
