from twisted.internet import reactor
from twisted.internet.task import Clock

def test():
    print("HI")

clock = Clock()
clock.callLater(5, test)
clock.advance(5)
reactor.run()
