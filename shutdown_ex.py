from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred, DeferredList
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.task import Clock


class UDPScraper(DatagramProtocol):

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.expect_connection_response = True
        # Timeout after 15 seconds if nothing received.
        self.timeout_seconds = 15
        self.clock = Clock()
        self.timeout = self.clock.callLater(self.timeout_seconds, self.on_error)

    def on_error(self):
        """
        This method handles everything that needs to be done when something during
        the UDP scraping went wrong.
        """
        raise RuntimeError("error")

    def stop(self):
        """
        Stops the UDP scraper and closes the socket.
        :return: A deferred that fires once it has closed the connection.
        """
        if self.timeout.active():
            self.timeout.cancel()
        if self.transport:
            return maybeDeferred(self.transport.stopListening)
        else:
            return defer.succeed(True)

    def startProtocol(self):
        """
        This function is called when the scraper is initialized.
        Initiates the connection with the tracker.
        """
        self.transport.connect(self.ip, self.port)
        self.udpsession.on_start()

    def write_data(self, data):
        """
        This function can be called to send serialized data to the tracker.
        :param data: The serialized data to be send.
        """
        self.transport.write(data) # no need to pass the ip and port

    def datagramReceived(self, data, (host, port)):
        """
        This function dispatches data received from a UDP tracker.
        If it's the first response, it will dispatch the data to the handle_connection_response
        function of the UDP session.
        All subsequent data will be send to the _handle_response function of the UDP session.
        :param data: The data received from the UDP tracker.
        """
        # Cancel the timeout
        if self.timeout.active():
            self.timeout.cancel()

    # Possibly invoked if there is no server listening on the
    # address to which we are sending.
    def connectionRefused(self):
        """
        Handles the case of a connection being refused by a tracker.
        """
        self.on_error()

scraper = UDPScraper('127.0.0.1', 13654)

def check_clean(_):
    delayed_calls = reactor.getDelayedCalls()
    if delayed_calls:
        print "DIRTY"
    else:
        print "CLEAN"
    reactor.stop()

def shutdown():
    stop_deferred = scraper.stop()
    stop_deferred.addCallback(check_clean)

print DeferredList([])

reactor.callLater(0.1, shutdown)
reactor.run()
