from time import sleep
from twisted.internet import reactor

from twisted.internet.threads import deferToThread

from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred


def done(x):
    print "DONE"
    reactor.stop()

def randomNum():
    sleep(1)
    return 42

@inlineCallbacks
def idunno():
    bla = yield deferToThread(randomNum)
    print 'hihi'
    returnValue(bla)


@inlineCallbacks
def crunch():
    for i in range(0, 10):
        print "Iteration ", i
        if i % 2 == 0:
            a = yield idunno()
            print a


class Koek:
    def print_this(self, x):
        print x

    def q(self):
        sleep(1)
        return 2

    @inlineCallbacks
    def two(self):
        two = yield self.q()
        returnValue("twee")

    @inlineCallbacks
    def three(self):
        bla = self.two()
        yield self.q()

        def one(x):
            self.print_this(x)

        bla.addCallback(one)

# for i in range(0, 4):
#     print "KOEK ", i
#     if i == 1:
#         d = crunch()
#         d.addCallback(done)


k = Koek()

d = k.three()


reactor.run()