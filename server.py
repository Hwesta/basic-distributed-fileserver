#! /usr/bin/python2
# TODO Remember to change this to python
# -*- test-case-name: a2.test.test_server -*-

# Twisted - networking library
from twisted.internet.protocol import ServerFactory, ClientCreator, DatagramProtocol
from twisted.protocols.basic import LineReceiver
from twisted.protocols.policies import TimeoutMixin
from twisted.internet.task import LoopingCall
from twisted.internet import reactor, defer

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
import json
import copy

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


class Heartbeat(DatagramProtocol, TimeoutMixin):
    # Primary server

    def __init__(self, msg, d):
        self.msg = msg
        self.loopObj = None
        self.deferred = d
        self.expect_rcv = False

    def startProtocol(self):
        "Called when transport is connected"
        self.transport.joinGroup("228.0.0.5")
        self.loopObj = LoopingCall(self.sendHeartBeat)
        self.loopObj.start(1, now=False)

    def stopProtocol(self):
        "Called after all transport is teared down"
        self.setTimeout(None)

    def datagramReceived(self, data, __):
        if data != self.msg:
            if verbosity > 4:
                print "Heartbeat received %r" % data
            if self.expect_rcv:
                self.resetTimeout()
                if verbosity > 4:
                    print 'Heartbeat reset timeout'
            else:
                self.setTimeout(2.5)  # seconds
                self.expect_rcv = True
                if verbosity > 4:
                    print 'Heartbeat started timeout'

    def timeoutConnection(self):
        if verbosity > 3:
            print 'Heartbeat timeout connection'
        self.expect_rcv = False
        if self.deferred is not None:
            new_d = defer.Deferred()
            d, self.deferred = self.deferred, new_d
            d.callback((new_d,))

    def sendHeartBeat(self):
        self.transport.write(self.msg, ("228.0.0.5", 8005))


class SyncProtocol(LineReceiver):
    """
    Reads and parses a message to the secondary server.

    Send: NEW_SEC, READ, SYNC_LOG, SEC_COMMIT, SEC_ABORT
    Format:
    -> METHOD txn_id seq length
    ->
    -> buf

    Receive: SYNC_FILES
    Format:
    -> METHOD length
    ->
    -> file_list

    Receive: ACK, ERROR
    Format:
    -> METHOD txn_id seq error_num length
    ->
    -> error_reason

    Receive: file contents
    Format:
    -> file_contents    
    """

    firstLine = True
    method = None
    length = None
    buf = None
    deferred = None

    def sendNEW_SEC(self, host, port, files):
        buf = "%s %d\r\n%s" % (host, port, files)
        msg = "NEW_SEC 0 0 %d\r\n\r\n%s\r\n" % (len(buf), buf)
        self.transport.write(msg)
        self.deferred = defer.Deferred()
        return self.deferred

    def sendREAD(self, fname):
        msg = "READ 0 0 %d\r\n\r\n%s\r\n" % (len(fname), fname)
        self.transport.write(msg)
        self.buf = ""
        self.setRawMode()
        self.deferred = defer.Deferred()
        return self.deferred

    def sendSYNC_LOG(self):
        msg = "SYNC_LOG 0 0 0\r\n\r\n\r\n"
        self.transport.write(msg)
        self.buf = ""
        self.setRawMode()
        self.deferred = defer.Deferred()
        return self.deferred

    def sendSEC_COMMIT(self, txn_id, seq, log):
        msg = "SEC_COMMIT %d %d %d\r\n\r\n%s\r\n" % (txn_id, seq, len(log), log)
        self.transport.write(msg)
        self.deferred = defer.Deferred()
        return self.deferred

    def sendSEC_ABORT(self, txn_id, seq, log):
        msg = "SEC_ABORT %d %d %d\r\n\r\n%s\r\n" % (txn_id, seq, len(log), log)
        self.transport.write(msg)
        self.deferred = defer.Deferred()
        return self.deferred

    def lineReceived(self, line):
        if verbosity > 3:
            print 'SyncProtocol rcv:', line
        # Many assumptions about incoming data, since from server
        if self.firstLine:
            self.firstLine = False

            l = line.split()
            if l[0] == "ERROR":
                self.transport.loseConnection()
                d, self.deferred = self.deferred, None
                d.errback(Exception(208, "Failed on other server. Line: %s" % line))
                return
            if len(l) == 2:
                self.method, length = l
            elif len(l) == 5:
                self.method, __, __, __, length = l
            self.length = int(length)
            return

        if not line:
            self.buf = ""
            self.setRawMode()

    def rawDataReceived(self, data):
        if self.length is None:
            self.buf += data
        if self.length is not None:
            data = data[:self.length]
            self.buf += data
            self.length -= len(data)
        if self.length == 0:
            self.transport.loseConnection()

    def connectionLost(self, reason):
        self.processMessage()

    def processMessage(self):
        if self.deferred is not None:
            d, self.deferred = self.deferred, None
            d.callback(self.buf)

# Messages parsing modeled off of twisted.web.http HTTPClient and HTTPChannel
class FilesystemProtocol(LineReceiver, TimeoutMixin):
    """
    Reads and parses a message to the primary server.

    Receive: NEW_TXN, WRITE, ABORT, COMMIT, READ, NEW_SEC, SYNC_LOG,
             SEC_COMMIT, SEC_ABORT
    Format:
    -> METHOD txn_id seq length
    ->
    -> buf

    Send: ERROR, ACK, ASK_RESEND
    Format:
    -> METHOD txn_id seq error_num length
    ->
    -> error_reason

    Send: file contents
    Format:
    -> file_contents

    Send: SYNC_FILES
    Format:
    -> METHOD length
    ->
    -> file_list
    """
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
        if verbosity > 3:
            print 'FilesystemProtocol rcv:', line
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
        if verbosity > 0:
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

    def sendSYNC_FILES(self, files):
        msg = "SYNC_FILES %d\r\n\r\n%s\r\n" % (len(files), files)
        self.transport.write(msg)
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
        elif self.method == "NEW_SEC":
            self.processNEW_SEC()
        elif self.method == "SYNC_LOG":
            self.processSYNC_LOG()
        elif self.method == "SEC_COMMIT":
            self.processSEC_COMMIT()
        elif self.method == "SEC_ABORT":
            self.processSEC_ABORT()
        else:
            self.sendError(204, "Method does not exist.")

    def processREAD(self):
        (error, buf) = self.factory.service.readFile(self.buf)
        if error != 0:
            self.sendError(error, buf)
        else:
            self.transport.write(buf)
            self.transport.loseConnection()

    def processNEW_TXN(self):
        (txn_id, error, error_reason) = self.factory.service.startNewTxn(self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        else:
            self.txn = txn_id
            self.sendACK()

    def processWRITE(self):
        (error, error_reason) = self.factory.service.saveWrite(self.txn, self.seq, self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        self.transport.loseConnection()

    def processABORT(self):
        d = self.factory.service.abortTxn(self.txn, self.seq)
        d.addCallbacks(self.commitSuccess, self.commitFail)

    def processCOMMIT(self):
        d = self.factory.service.commitTxn(self.txn, self.seq)
        d.addCallbacks(self.commitSuccess, self.commitFail)

    def processNEW_SEC(self):
        (peer, files) = self.buf.split('\r\n', 1)
        (host, port) = peer.split()
        if verbosity > 2:
            print "Files from secondary:", files
        diff_files = self.factory.service.addSecondary(host, int(port), files)
        self.sendSYNC_FILES(diff_files)

    def processSYNC_LOG(self):
        log = self.factory.service.syncLog()
        self.transport.write(log)
        self.transport.loseConnection()

    def processSEC_COMMIT(self):
        d = self.factory.service.writeLog('COMMIT', self.txn, self.seq, self.buf)
        d.addCallbacks(self.commitSuccess, self.commitFail)

    def processSEC_ABORT(self):
        d = self.factory.service.writeLog('ABORT', self.txn, self.seq, self.buf)
        d.addCallbacks(self.commitSuccess, self.commitFail)

    def commitSuccess(self, (action, writes)):
        if action == 'ASK_RESEND':
            self.sendASK_RESEND(writes)
        elif action == 'ACK':
            self.sendACK()
        else:
            print 'WTF ERROR', action, writes

    def commitFail(self, reason):
        try:
            (error, error_reason) = reason.value
            self.sendError(error, error_reason)
        except Exception, e:
            print "WTF ERROR", e
            self.sendError(209, "An unknown error occurred.")


class FilesystemService():
    """ Provides the filesystem functionality: writes and logs transactions. """

    logdir = ".server_log/"
    logfile = logdir+"log"
    lock_prefix = ".lock-"

    txn_list = None
    file_list = {}
    primary_txt = None

    role = None
    primary = None  # None if this is the primary
    secondary = None  # None if this is a secondary

    host = None
    port = None
    heartbeatd = None  # Deferred passed to heartbeat
    sync_port = None  # Port the secondary listens on for syncing
    sync_factory = None  # Link to factory for syncing

    # Run on server startup
    def __init__(self, args):
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
            self.primary_txt = args.primary

        self.host = args.ip
        self.port = args.port

        # Create log directory
        try:
            os.makedirs(self.logdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
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

        # Read primary.txt
        try:
            f = open(self.primary_txt)
        except:
            print "Primary.txt could not be opened."
            sys.exit(-1)

        # Determine role from primary.txt
        l = f.readline()
        try:
            host, port = l.split()
            port = int(port)
            if host == self.host and port == self.port:
                self.becomePrimary()
            elif host == 'localhost' or host == '127.0.0.1':
                print "Cannot run on localhost.  Exiting."
                sys.exit(-1)
            else:
                self.becomeSecondary((host,port))
        except ValueError:
            self.becomePrimary()
        finally:
            f.close()

        # Start listening
        factory = ServerFactory()
        factory.protocol = FilesystemProtocol
        factory.service = self
        reactor.listenTCP(self.port, factory, interface=self.host)



    # Run on server shutdown
    def __del__(self):
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
        """
        Generates a dictionary of filenames and their hashes known to the server.
        """
        # from http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python
        # def md5_for_file(f, block_size=2**20):
        #     md5 = hashlib.md5()
        #     while True:
        #         data = f.read(block_size)
        #         if not data:
        #             break
        #         md5.update(data)
        #     return md5.hexdigest()
        hashs = [(f, hashlib.md5(open(f, 'r').read()).hexdigest())
                 for f in os.listdir('.')
                 if not os.path.isdir(f) and f[0] != '.'
                 ]
        self.file_list = dict(hashs)
        if verbosity > 1:
            print "Hashes:", self.file_list

    def becomePrimary(self):
        """
        Become primary server. Run by secondary to switch roles, or on boot.
        """
        if self.role == 'PRIMARY':
            print 'ERR tried to become primary twice'
            return
        # Cleanup from being secondary, if applicable
        self.primary = None
        # Become primary
        self.role = 'PRIMARY'
        self.secondary = None

        self.hashFiles()

        # Write to primary.txt
        with open(self.primary_txt, 'w') as f:
            buf = "%s %d" % (self.host, self.port)
            if verbosity > 1:
                print 'writing to primary.txt', buf
            f.write(buf)

        if verbosity > 0:
            print "Role:", self.role
            print 'Log:', self.txn_list

        # Setup heartbeat
        self.setupHeartbeat()
        self.heartbeatd.addCallback(self.removeSecondary)

    @defer.inlineCallbacks
    def becomeSecondary(self, primary):
        """ Become secondary server.  Run on boot. """
        if self.role != None:
            print "ERROR: Becoming secondary, previous roles %s" % self.role
        self.role = 'SECONDARY'
        self.primary = primary
        if verbosity > 0:
            print "Role:", self.role
            print "Primary:", primary

        self.hashFiles()

        # Remove NEW_TXNs if secondary
        for (txn_id, txn) in self.txn_list.items():
            if (txn_id != 'next_id' and txn['status'] == 'NEW_TXN'):
                del self.txn_list[str(txn_id)]
        if verbosity > 0:
            print 'Log:', self.txn_list

        # Sync files with primary
        try:
            protocol = yield self.connectToServer(self.primary)
        except Exception, e:
            if verbosity > 0:
                print "SEC Could not connect to primary. Becoming primary."
            self.becomePrimary()
            return
        if verbosity > 1:
            print "SEC files:", self.file_list
        j = json.dumps(self.file_list)
        j = yield protocol.sendNEW_SEC(self.host, self.port, j)

        files = json.loads(j)
        files = map(str, files)
        if verbosity > 1:
            print "SEC getting files:", files
        for fname in files:
            try:
                protocol = yield self.connectToServer(self.primary)
                buf = yield protocol.sendREAD(fname)
            except Exception, e:
                self.lostPrimary()
                return

            if verbosity > 2:
                print "SEC writing file:", fname
                print "SEC writing data:", buf
            with open(fname, 'w') as f:
                f.write(buf)

        # Sync log
        try:
            protocol = yield self.connectToServer(self.primary)
        except Exception, e:
            if verbosity > 0:
                print "SEC Could not connect to primary. Becoming primary."
            self.becomePrimary()
            return
        j = yield protocol.sendSYNC_LOG()
        new_log = json.loads(j)
        new_log = dict([(str(k), new_log[k]) for k in new_log])
        if verbosity > 1:
            print "Log items from primary:", new_log
        for txn_id in new_log:
            txn = new_log[txn_id]
            txn = dict([(str(k), txn[k]) for k in txn])
            txn['writes'] = dict([(int(k), txn['writes'][k]) for k in txn['writes']])
            self.txn_list[txn_id] = txn

        if verbosity > 0:
            print "Log:", self.txn_list

        # Setup heartbeat
        self.setupHeartbeat()
        self.heartbeatd.addCallback(self.lostPrimary)

    def connectToServer(self, (host,port)):
        """ Establish connection from secondary to primary. Return Deferred. """
        connection = ClientCreator(reactor, SyncProtocol)
        d = connection.connectTCP(host, port)
        return d

    def setupHeartbeat(self):
        if self.heartbeatd is None:
            self.heartbeatd = defer.Deferred()
            msg = "%s:%d" % (self.host, self.port)
            protocol = Heartbeat(msg, self.heartbeatd)
            reactor.listenMulticast(8005, protocol, listenMultiple=True)

    def lostPrimary(self, (d,)):
        if verbosity > 0:
            print "SEC heartbeat failed. Becoming primary."
        self.heartbeatd = d
        self.becomePrimary()

    def addSecondary(self, host, port, files):
        """
        Adds a secondary to the primary.  Returns a list of files that differ,
        comparing the provided list and the primary's files.
        """
        self.secondary = (host,port)
        if verbosity > 0:
            print "PRI added secondary", host, port

        # Compare given files with own files
        sec_files = json.loads(files)
        diff_files = {}
        sec_files = dict([(str(k), sec_files[k]) for k in sec_files])

        for local_file in self.file_list:
            if local_file not in sec_files or \
                self.file_list[local_file] != sec_files[local_file]:
                diff_files[local_file] = None

        if verbosity > 1:
            print "PRI differing files:", diff_files
        j = json.dumps(diff_files)
        return j

    def removeSecondary(self, (d,)):
        if verbosity > 0:
            print "PRI removing secondary"  # host, port
        # TODO do this properly
        self.secondary = None
        self.heartbeatd = d
        self.heartbeatd.addCallback(self.removeSecondary)

    def syncLog(self):
        log = {}
        for (txn_id, txn) in self.txn_list.items():
            if (txn_id != 'next_id' and
                (txn['status'] == 'COMMIT' or txn['status'] == 'ABORT')):
                log[txn_id] = self.txn_list[str(txn_id)]
        j = json.dumps(log)
        return j

    def readFile(self, file_name):
        """ Read file_name. """
        if self.role == 'SECONDARY':
            return (207, "Connect to primary at %s:%d" % self.primary)
        if not os.path.isfile(file_name):
            return (206, "File not found.")
        if verbosity > 1:
            print "READ", file_name

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
        """ Create and log a new transaction on new_file. """
        # Error checking
        if self.role == 'SECONDARY':
            return (0, 207, "Connect to primary at %s:%d" % self.primary)
        if os.path.isdir(new_file):
            return (0, 205, "A directory with that name already exists.")
        if new_file[0] == '.':
            return (0, 202, "Creating hidden files is forbidden.")
        if '/' in new_file:
            return (0, 202, "Creating directories (or files in subdirectories) is forbidden.")

        # New transaction information
        # TODO TEST THIS
        txn_id = self.txn_list['next_id']
        while (str(txn_id) in self.txn_list):
            txn_id+=1

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

        if verbosity > 0:
            print 'Log:', self.txn_list[str(txn_id)]
        return (txn_id, 0, None)

    def saveWrite(self, txn_id, seq, buf):
        """ Add a write to txn_id identified by seq with data buf. """
        # Error checking
        if self.role == 'SECONDARY':
            return (207, "Connect to primary at %s:%d" % self.primary)
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

        if verbosity > 0:
            print 'Log:', self.txn_list[str(txn_id)]
        return (0, None)

    @defer.inlineCallbacks
    def abortTxn(self, txn_id, seq, override=False):
        """ Abort transaction identified by txn_id. """
        # Error checking
        if self.role == 'SECONDARY' and not override:
            raise Exception (207, "Connect to primary at %s:%d" % self.primary)
        if str(txn_id) not in self.txn_list:
            raise Exception (201, "Unknown transaction id.")
        txn_info = self.txn_list[str(txn_id)]

        if txn_info['status'] == 'COMMIT':
            raise Exception (202, "Transaction has been comitted already.")

        # Sync to secondary
        if self.secondary is not None:
            try:
                protocol = yield self.connectToServer(self.secondary)
            except:
                if verbosity > 0:
                    print "No secondary, despite heartbeat."
            else:
                cb_txn_info = copy.deepcopy(txn_info)
                j = json.dumps(txn_info)
                # needs error checking
                yield protocol.sendSEC_ABORT(txn_id, seq, j)

        # Update status
        txn_info['status'] = 'ABORT'

        # Write to log, write out
        self.txn_list[str(txn_id)] = txn_info
        self.txn_list.sync()

        if verbosity > 0:
            print 'Log:', self.txn_list[str(txn_id)]

        defer.returnValue( ('ACK', None) )

    @defer.inlineCallbacks
    def commitTxn(self, txn_id, seq, override=False):
        """ Commit transaction identified by txn_id. """
        # Error checking
        if self.role == 'SECONDARY' and not override:
            raise Exception(207, "Connect to primary at %s:%d" % self.primary)
        if str(txn_id) not in self.txn_list:
            raise Exception(201, "Unknown transaction id.")
        txn_info = self.txn_list[str(txn_id)]

        if txn_info['status'] == 'ABORT':
            raise Exception(202, "Transaction has been aborted already.")
        elif txn_info['status'] == 'COMMIT':  # Don't recheck sequence number
            defer.returnValue( ('ACK', None) )

        # Check that have right number of writes
        unsent = [k for k in range(0,seq) if k not in txn_info['writes']]
        if len(unsent) != 0:
            defer.returnValue( ('ASK_RESEND', unsent) )

        # Write to the file

        filename = txn_info['file']
        lock_file = self.logdir + self.lock_prefix + filename  # Make this a hash??
        data = "".join(
            [txn_info['writes'][k] for k in txn_info['writes'] if k < seq])

        # If the lock file exists, another transaction is comitting
        # TODO Use deferred here
        while os.path.isfile(lock_file):
            print 'locked', txn_id
            time.sleep(0.05)

        # Sync to secondary
        if self.secondary is not None:
            try:
                protocol = yield self.connectToServer(self.secondary)
            except:
                if verbosity > 0:
                    print "No secondary, despite heartbeat."
            else:
                cb_txn_info = copy.deepcopy(txn_info)
                j = json.dumps(txn_info)
                # needs error checking
                yield protocol.sendSEC_COMMIT(txn_id, seq, j)

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
            txn_info['writes_committed'] = seq

            # Copy back
            shutil.move(lock_file, filename)
        except:
            raise Exception(205,
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

        # Update file hash
        self.file_list[filename] = hashlib.md5(open(filename, 'r').read()).hexdigest()

        if verbosity > 0:
            print 'Log:', self.txn_list[str(txn_id)]

        defer.returnValue( ('ACK', None) )

    @defer.inlineCallbacks
    def writeLog(self, action, txn_id, seq, log):
        j = json.loads(log)
        # Convert dict keys from unicode strings to correct type
        j = dict([(str(k), j[k]) for k in j])
        j['writes'] = dict([(int(k), j['writes'][k]) for k in j['writes']])
        self.txn_list[str(txn_id)] = j
        if action == 'COMMIT':
            (result, reason) = yield self.commitTxn(txn_id, seq, override=True)
        elif action == 'ABORT':
            (result, reason) = yield self.abortTxn(txn_id, seq, override=True)
        defer.returnValue( (result, reason) )


def main():
    args = parse_args()

    service = FilesystemService(args)

    reactor.run()

# Start the server
if __name__ == '__main__':
    main()
