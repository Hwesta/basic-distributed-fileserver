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
import os, sys, errno
import shelve # For writing out dictionary
import shutil # For file copy

# Messages parsing modeled off of twisted.web.http HTTPClient and HTTPChannel
class Server(LineReceiver, TimeoutMixin):

    method = None
    txn = None
    seq = None
    length = None
    buf = None
    data = True # Is there data for this message?
    firstLine = True

    def connectionMade(self):
        self.setTimeout(3) # seconds

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
            #print "DEBUG: Header received:", self.method, self.txn, self.seq, self.length
            return
        # No expected data
        if not self.data:
            self.processMessage()
        # Blank line - prepare to process data
        if not line:
            if self.length == 0: # COMMIT or ABORT
                self.data = False
                return
            self.buf = ""
            self.setRawMode() # Data arrives at rawDataReceived

    def rawDataReceived(self, data):
        if self.length is not None:
            data, rest = data[:self.length], data[self.length:]
            self.buf += data
            self.length -= len(data)
        if self.length == 0:
            self.processMessage()
            self.setLineMode(rest)

    def timeoutConnection(self):
        print "Timing out client: %s" % str(self.transport.getPeer())
        self.sendError(204, "Connection timed out (is length longer than data?)")

    def sendError(self, err_num, err_reason):
        # 201 - Invalid transaction ID. Sent by the server if the client had sent a message that included an invalid transaction ID, i.e., a transaction ID that the server does not remember
        # 202 - Invalid operation. Sent by the server if the client attemtps to execute an invalid operation - i.e., write as part of a transaction that had been committed
        # 204 - Wrong message format. Sent by the server if the message sent by the client does not follow the specified message format
        # 205 - File I/O error
        # 206 - File not found (read)

        if not isinstance(self.txn, int):
            self.txn = -1
        error = "ERROR %d 0 %d %d\r\n\r\n%s" % (self.txn, err_num, len(err_reason), err_reason)
        self.sendLine(error)
        # self.transport.write(error)
        self.transport.loseConnection()

    def sendACK(self, txn_id):
        ack = "ACK %d 0 0 0\r\n\r\n\r\n" % (txn_id)
        self.sendLine(ack)
        self.transport.loseConnection()

    # TEST THIS
    def sendASK_RESEND(self, txn_id, missing_writes):
        resend_string = "ASK_RESEND %d %d 0 0\r\n\r\n\r\n"
        for write in missing_writes:
            resend = resend_string % (txn_id, write)
            self.sendLine(resend)
        self.transport.loseConnection()

    def processMessage(self):
        self.setTimeout(None)
        self.sendLine("Method: %s, txn: %d, seq: %d, buf: %s\0" % (self.method, self.txn, self.seq, self.buf))
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
        self.transport.loseConnection()

    def processNEW_TXN(self):
        (txn_id, error, error_reason) = self.factory.startNewTxn(self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        else:
            self.sendACK(txn_id)

    def processWRITE(self):
        (error, error_reason) = self.factory.saveWrite(self.txn, self.seq, self.buf)
        if error != 0:
            self.sendError(error, error_reason)
        self.transport.loseConnection()

    def processCOMMIT(self):
        pass

    def processABORT(self):
        pass



class ServerFactory(Factory):
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
        print "cwd", os.getcwd()

        # Create hidden log dir
        self.logdir = ".server_log/"
        try:
            os.makedirs(self.logdir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                print "Could not initialize server.  Does the directory have execute permission?"
                sys.exit(-1)

        self.logfile = self.logdir+"log"
        # If logs exist, read from disk
        if os.path.isfile(self.logfile):
            log = shelve.open(self.logfile)
        else:
            print 'log dne'
            log = shelve.open(self.logfile)
            log['next_id'] = 1
        self.txn_list = log
        print 'self txn list', self.txn_list

    def __del__(self):
        if self.txn_list is not None:
            self.txn_list.close()
        # Delete log files, since it was a 'graceful' shutdown?

    def startNewTxn(self, new_file):
        txn_id = self.txn_list['next_id']
        txn_info = {'file': new_file, 'status': 'NEW_TXN', 'writes': {}}
        # temp_file = self.logdir+new_file+str(txn_id)

        # CHECK Do I need to create the log file?
        # Open/Create file
        if os.path.isdir(new_file): # Directory - error
            print "dir"
            return txn_id, 205, "A directory with that name already exists."
        # elif os.path.isfile(new_file): # Existing file, copy to log dir
        #     print "existing file"
        #     try:
        #         shutil.copy2(new_file, temp_file)
        #     except:
        #         return txn_id, 205, "File IO error.  Check server settings and permissions."
        # else: # New file - create log file
        #     print "new file"
        #     try:
        #         f = open(temp_file, 'w+')
        #     except:
        #         return txn_id, 205, "File IO error.  Check server settings and permissions."
        #     f.close()
        
        # Update log
        self.txn_list['next_id']=txn_id+1
        self.txn_list[str(txn_id)] = txn_info
        print self.txn_list

        # Flush log to disk
        self.txn_list.sync()
        self.txn_list.sync()
 
        return txn_id, 0, None

    def saveWrite(self, txn_id, seq, buf):
        if str(txn_id) not in self.txn_list:
            return (201, "Unknown transaction id.")
        else:
            txn_info = self.txn_list[str(txn_id)]

        if seq < 0:
            return (204, "Sequence number has to be a positive integer.")

        txn_info['writes'][seq] = buf
        self.txn_list[str(txn_id)] = txn_info
        print self.txn_list
        self.txn_list.sync()

        return 0, None

    def commitTxn(self):
        pass
        # Check that have right number of things
        # if all (str(k) in self.txn_list for k in range(seq)):


def runserver():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run a distributed fileserver.')
    parser.add_argument('-ip', default='127.0.0.1', help="IP of the server.")
    parser.add_argument('-port', default=8080, type=int, help="Port the server runs on.")
    parser.add_argument('-dir', required=True, help='Directory to store files in.')
    args = parser.parse_args()

    reactor.listenTCP(args.port, ServerFactory(args.dir), interface=args.ip)
    reactor.run()

# Start the server
if __name__ == '__main__':
    runserver()
 



