from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor

class MultiEcho(Protocol):
    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        self.factory.echoers.append(self)

    def dataReceived(self, data):
        for echoer in self.factory.echoers:
            echoer.transport.write(data)

    def connectionLost(self, reason):
        self.factory.echoers.remove(self)


class MultiEchoFactory(Factory):
    def __init__(self):
        self.echoers = []

    def buildProtocol(self, addr):
        return MultiEcho(self)

reactor.listenTCP(4321, MultiEchoFactory())
reactor.run()
