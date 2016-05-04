import json
import os
import requests
import sqlite3
import time
import zipfile
import math

import StringIO

import sys
from twisted.internet.endpoints import TCP4ServerEndpoint

from twisted.web._newclient import ResponseNeverReceived

from twisted.internet.error import ConnectingCancelledError

from twisted.internet import reactor
from twisted.internet.defer import DeferredList, maybeDeferred, CancelledError, Deferred, setDebugging, gatherResults, \
    succeed
from twisted.internet.threads import deferToThread
from twisted.web import resource, server
from twisted.web.client import Agent, readBody


class Benchmark:
    NUM_CONNECTIONS = 1
    INITIAL_PORT = 13212

    def __init__(self):
        pass

    def start_experiment(self):
        reactor.callWhenRunning(self.create_table)
        reactor.run()

    def create_table(self):
        db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db")
        self.conn = sqlite3.connect(db_filename)
        cursor = self.conn.cursor()
        data_table_creation = """create table data (
                        indexString text,
                        squared INTEGER ,
                        cubed INTEGER);"""

        ins_table_creation = """create table ins (
                        original INTEGER,
                        squared INTEGER ,
                        cubed INTEGER);"""

        # Ensure to delete the old test table if it exists
        cursor.execute("DROP TABLE IF EXISTS data")
        cursor.execute("DROP TABLE IF EXISTS ins")
        cursor.execute(data_table_creation)
        cursor.execute(ins_table_creation)
        self.conn.commit()

        self.insert_data()

    def insert_data(self):
        print "starting inserting..."
        cursor = self.conn.cursor()
        for i in xrange(1000000):
            cursor.execute("INSERT INTO data VALUES(?, ?, ?)", (str(i).rjust(6, '0'), i ** 2, i ** 3))
        self.conn.commit()
        print "done inserting."
        # cursor = self.conn.cursor()
        # zip_file = os.path.join(os.getcwd(), "benchmark.zip")
        # with open(zip_file, "rb") as input_file:
        #     ablob = input_file.read()
        #     cursor.execute("INSERT INTO data (File, Type, FileName) VALUES(?, 'zip', '" + zip_file + "')", [sqlite3.Binary(ablob)])
        #     self.conn.commit()
        self.run()

    def run(self):
        def start_async(_):
            print "blocking: %d" % (time.time() * 1e6 - self.start)
            sys.stdout.flush()
            self.start = time.time() * 1e6
            print "STARTING ASYNCHRONOUS SERVER-CLIENT BENCHMARK"
            async_deferred = self.start_simulations(False)
            async_deferred.addCallback(self.tear_down)

        print "STARTING SYNCHRONOUS SERVER-CLIENT BENCHMARK"
        self.start = time.time() * 1e6
        blocking_deferred = self.start_simulations(True)  # One run with blocking code
        blocking_deferred.addCallback(start_async)

    def tear_down(self, _):
        print "async time: %d" % (time.time() * 1e6 - self.start)
        reactor.stop()

    def start_simulations(self, blocking):
        deferred_list = []

        for i in xrange(self.NUM_CONNECTIONS):
            simulation = ClientServerSimulation(blocking)
            deferred_list.append(simulation.done_deferred)
            simulation.perform_simulation()

        return DeferredList(deferred_list)


class ClientServerSimulation():
    NUM_REQUESTS = 3

    def __init__(self, blocking):
        self.blocking = blocking
        self.site = server.Site(SimpleAPI(blocking))
        self.port = reactor.listenTCP(0, self.site)
        self.num_port = self.port.getHost().port
        print "One client running on port %s" % self.num_port

        self.url = "http://localhost:%s/zip" % self.num_port
        self.count = 0
        self.deferreds = []
        setDebugging(True)

        db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db")
        self.conn = sqlite3.connect(db_filename, check_same_thread=False)

        self.done_deferred = Deferred()

    def perform_simulation(self):
        return self.prepare_server().addCallback(self.run_requests)

    def prepare_server(self):
        ep = TCP4ServerEndpoint(reactor, 0)

        def onStartedListening(port):
            self.port = port
            return "http://localhost:%s/zip" % port.getHost().port

        return ep.listen(self.site).addCallback(onStartedListening)

    def run_requests(self, url):
        parametered_url = url + ("?i=" + str(self.count % 10) )
        for i in range(self.NUM_REQUESTS):
            if self.blocking:
                print "here"
                r = requests.get(parametered_url, timeout=10)
                data = self.parse_body(r.text)
                self.parse_data(data)
                self.deferreds.append(succeed(True))
            else:
                agent = Agent(reactor, connectTimeout=3600)
                request = agent.request('GET', parametered_url)
                request.addCallbacks(self.on_response, self.on_error)  # Return the readBody deferred
                request.addCallbacks(self.parse_body, self.on_error)
                request.addCallbacks(self.parse_data, self.on_error)
                self.deferreds.append(request)

        # Wait for all requests to complete and be processed, then call de done_deferred.
        return gatherResults(self.deferreds).addCallback(self.stop)

    def on_response(self, response):
        return readBody(response)

    def parse_body(self, body):
        # Deflate the zip - CPU intensive
        in_memory_zip = StringIO.StringIO(body)
        zf = zipfile.ZipFile(in_memory_zip, 'r')
        data = zf.open("data.json").read()
        return data

    def parse_data(self, data):
        json_data = json.loads(data)
        data_to_insert = []
        for obj in json_data:
            squared = obj["squared"]
            cubed = obj["cubed"]
            original = math.sqrt(squared)
            data_to_insert.append((original, squared, cubed))
        self.insert_ins_values(data_to_insert)

    def insert_ins_values(self, data_list):
        cursor = self.conn.cursor()
        cursor.executemany("INSERT INTO ins VALUES(?, ?, ?)", data_list)
        self.conn.commit()

    def on_error(self, failure):
        failure.trap((ConnectingCancelledError, CancelledError, ResponseNeverReceived))
        print failure

    def stop(self, _):
        print "stopping %s, %s" % (self.num_port, self.count)
        return maybeDeferred(self.port.stopListening).chainDeferred(self.done_deferred)



if __name__ == "__main__":
    b = Benchmark()
    b.start_experiment()
