#! /usr/bin/python2
# TODO Remember to change this to python

# Twisted - networking library
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

# Other
import argparse

class Chat(LineReceiver):

    def __init__(self, users):
        self.users = users
        self.name = None
        self.state = "GETNAME"

    def connectionMade(self):
        self.sendLine("What's your name?")

    def connectionLost(self, reason):
        if self.users.has_key(self.name):
            del self.users[self.name]

    def lineReceived(self, line):
        if self.state == "GETNAME":
            self.handle_GETNAME(line)
        else:
            self.handle_CHAT(line)

    def handle_GETNAME(self, name):
        if self.users.has_key(name):
            self.sendLine("Name taken, please choose another.")
            return
        self.sendLine("Welcome, %s!" % (name,))
        self.name = name
        self.users[name] = self
        self.state = "CHAT"

    def handle_CHAT(self, message):
        message = "<%s> %s" % (self.name, message)
        for name, protocol in self.users.iteritems():
            if protocol != self:
                protocol.sendLine(message)


class ChatFactory(Factory):

    def __init__(self):
        self.users = {} # maps user names to Chat instances

    def buildProtocol(self, addr):
        return Chat(self.users)
  

def runserver():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run a distributed fileserver.')
    parser.add_argument('-ip', default='127.0.0.1', help="IP of the server.")
    parser.add_argument('-port', default=8080, type=int, help="Port the server runs on.")
    parser.add_argument('-dir', required=True, help='Directory to store files in.')
    args = parser.parse_args()

    reactor.listenTCP(args.port, ChatFactory(), interface=args.ip)
    reactor.run()

# Start the server
if __name__ == '__main__':
    runserver()



