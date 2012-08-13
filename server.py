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

# Messages parsing modeled off of twisted.web.http HTTPClient and HTTPChannel
class Server(LineReceiver, TimeoutMixin):

    method = None
    txn = None
    seq = None
    length = None
    buf = None
    data = True
    firstLine = True

    def __init__(self, dir):
        self.dir = dir

    def connectionMade(self):
        self.setTimeout(3)

    def connectionLost(self, reason):
        self.setTimeout(None)

    def lineReceived(self, line):
        self.resetTimeout()
        if self.firstLine:
            self.firstLine = False
            l = line.split()
            if len(l) != 4:
                self.send_error(204, "Header is wrong length")
                return
            self.method, self.txn, self.seq, self.length = l
            try:
                self.txn = int(self.txn)
                self.seq = int(self.seq)
                self.length = int(self.length)
            except ValueError:
                self.send_error(204, "Header has non-numeric value")
                return
            #print "DEBUG: Header received:", self.method, self.txn, self.seq, self.length
            return
        # Second empty line, and no expected data
        if not self.data:
            self.process_message()
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
            self.process_message()
            self.setLineMode(rest)

    def timeoutConnection(self):
        #print "Timing out client: %s" % str(self.transport.getPeer())
        self.send_error(204, "Connection timed out (length longer than data?)")

    def send_error(self, err_num, err_reason):

        # 201 - Invalid transaction ID. Sent by the server if the client had sent a message that included an invalid transaction ID, i.e., a transaction ID that the server does not remember
        # 202 - Invalid operation. Sent by the server if the client attemtps to execute an invalid operation - i.e., write as part of a transaction that had been committed
        # 204 - Wrong message format. Sent by the server if the message sent by the client does not follow the specified message format
        # 205 - File I/O error
        # 206 - File not found

        if not isinstance(self.txn, int):
            self.txn = -1
        error = "ERROR %d 0 %d %d\r\n\r\n%s" % (self.txn, err_num, len(err_reason), err_reason)
        self.sendLine(error)
        self.transport.loseConnection()

    def process_message(self):
        self.setTimeout(None)
        self.sendLine("Method: %s, txn: %d, seq: %d, buf: %s" % (self.method, self.txn, self.seq, self.buf))
 


class ServerFactory(Factory):

    def __init__(self, dir):
        self.dir = dir

    def buildProtocol(self, addr):
        return Server(self.dir)


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

# Store:
#   Current directory
#   transaction IDs, associated file (dict?)  Log to file? (as JSON?)
#   



