import requests

from twisted.internet import reactor
from twisted.internet.defer import setDebugging, maybeDeferred, gatherResults, succeed
from twisted.web.client import Agent, readBody


class ClientServerSimulation():
    NUM_REQUESTS = 1

    def __init__(self, port):

        self.agent = Agent(reactor, connectTimeout=3600)
        self.deferreds = []
        setDebugging(True)
        self.port = port
        self.url = "http://localhost:%s/test" % port

    def run_requests(self):
        for i in range(self.NUM_REQUESTS):
            # THIS WORKS:
            # request = self.agent.request('GET', url)
            # request.addCallbacks(self.on_response, self.on_error) # Return the readBody deferred
            # request.addCallbacks(self.parse_body, self.on_error)
            # self.deferreds.append(request)

            # THIS TIMES OUT?
            print "before request"
            s = requests.Session()
            # r = requests.get(url, timeout=10)
            r = s.get(self.url, timeout=10)
            print "after request"
            data = self.parse_body(r.text)
            self.deferreds.append(succeed(True))

        # Wait for all requests to complete and be processed, then call de done_deferred.
        return gatherResults(self.deferreds).addCallback(self.stop)

    def on_response(self, response):
        print "in on_response"
        # deferred = readBody(response)
        # deferred.addCallbacks(self.parse_body, self.on_error)
        return readBody(response)


    def parse_body(self, body):
        print "Body: %s" % body

    def on_error(self, failure):
        print failure

    def stop(self, _):
        reactor.stop()

def start_experiment():
    INITIAL_PORT = 46527
    NUM_CLIENTS = 1
    for i in range(NUM_CLIENTS):
        c = ClientServerSimulation(INITIAL_PORT + i)
        c.run_requests()

if __name__ == "__main__":
    reactor.callWhenRunning(start_experiment)
    reactor.run()
