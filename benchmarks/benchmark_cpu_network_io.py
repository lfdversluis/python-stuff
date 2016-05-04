import StringIO
import itertools
import json
import math
import os
import requests
import shlex
import sqlite3
import subprocess
import time
import zipfile

from twisted.internet import reactor
from twisted.internet.defer import DeferredList, CancelledError, Deferred, setDebugging, gatherResults, \
    succeed
from twisted.internet.error import ConnectingCancelledError
from twisted.internet.threads import deferToThread
from twisted.web._newclient import ResponseNeverReceived
from twisted.web.client import Agent, readBody

NUM_CLIENTS = 6
NUM_REQUESTS = 1
INITIAL_PORT = 46527

class Benchmark:

    def __init__(self, conf):
        self.conf = conf

    def prepare_experiment(self):
        self.done_deferred = Deferred()

    def create_table(self, i):
        db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db_%s" % i)
        self.conn = sqlite3.connect(db_filename, check_same_thread=False)
        cursor = self.conn.cursor()
        ins_table_creation = """create table ins (
                        original INTEGER,
                        squared INTEGER ,
                        cubed INTEGER);"""

        # Ensure to delete the old test table if it exists
        cursor.execute("DROP TABLE IF EXISTS ins")
        cursor.execute(ins_table_creation)
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def run(self):
        self.start = time.time() * 1e6
        done_deferred = self.make_requests()  # One run with blocking code
        return done_deferred.addCallback(self.tear_down)

    def tear_down(self, _):
        total_time = (time.time() * 1e6 - self.start)
        with open("../data/benchmark_cpu_network_io.txt", "a") as data_file:
            data_file.write("%s %s %s %s\n" % (self.conf[0], self.conf[1], self.conf[2], total_time))

        self.done_deferred.callback(True)

    def make_requests(self):
        deferred_list = []

        for i in xrange(NUM_CLIENTS):
            self.create_table(i)
            simulation = ClientServerSimulation(i, self.conf, INITIAL_PORT + i)
            deferred_list.append(simulation.done_deferred)
            simulation.run_requests()

        return DeferredList(deferred_list)


class ClientServerSimulation():

    def __init__(self, i, conf, port):
        self.cpu_blocking = conf[0]
        self.net_blocking = conf[1]
        self.io_blocking = conf[2]
        print "One client running on port %s" % port
        self.num = i
        self.url = "http://localhost:%s/zip" % port
        self.count = 0
        self.deferreds = []
        setDebugging(True)

        self.done_deferred = Deferred()

    def run_requests(self):
        parametered_url = self.url + ("?i=" + str(self.count % 10))
        for i in range(NUM_REQUESTS):
            if self.net_blocking:
                r = requests.get(parametered_url, timeout=3600)
                # data = self.parse_body(r.text)
                # self.parse_data(data)
                self.deferreds.append(self.parse_body(r.text).addCallbacks(self.parse_data, self.on_error).addCallbacks(self.insert_ins_values, self.on_error))
            else:
                agent = Agent(reactor, connectTimeout=3600)
                request = agent.request('GET', parametered_url)
                request.addCallbacks(self.on_response, self.on_error)  # Return the readBody deferred
                request.addCallbacks(self.parse_body, self.on_error)
                request.addCallbacks(self.parse_data, self.on_error)
                request.addCallbacks(self.insert_ins_values, self.on_error)
                self.deferreds.append(request)

        # Wait for all requests to complete and be processed, then call de done_deferred.
        return gatherResults(self.deferreds).chainDeferred(self.done_deferred)

    def on_response(self, response):
        # print "got response"
        return readBody(response)

    def parse_body(self, body):
        # print "parsing body"
        # Deflate the zip - CPU intensive
        def unzip(body):
            in_memory_zip = StringIO.StringIO(body)
            zf = zipfile.ZipFile(in_memory_zip, 'r')
            data = zf.open("data.json").read()
            return data

        if self.cpu_blocking:
            return succeed(unzip(body))
        else:
            return deferToThread(unzip, body)

    def parse_data(self, data):

        def parse(data):
            json_data = json.loads(data)
            data_to_insert = []
            for obj in json_data:
                squared = obj["squared"]
                cubed = obj["cubed"]
                original = math.sqrt(squared)
                data_to_insert.append((original, squared, cubed))
            return data_to_insert

        if self.cpu_blocking:
            return succeed(parse(data))
        else:
            return deferToThread(parse, data)

    def insert_ins_values(self, data_list):
        # print "trying to insert..."
        def insert(data_list):
            db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db_%s" % self.num)
            self.conn = sqlite3.connect(db_filename, check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.executemany("INSERT INTO ins VALUES(?, ?, ?)", data_list)
            cursor.close()
            self.conn.commit()
            self.conn.close()
            # self.conn.commit()

        if self.io_blocking:
            return succeed(insert(data_list))
        else:
            return deferToThread(insert, data_list)

    def on_error(self, failure):
        failure.trap((ConnectingCancelledError, CancelledError, ResponseNeverReceived))
        print failure


# General stuff

def execute(command):
    popen = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=False)
    stdout_lines = iter(popen.stdout.readline, "")
    for stdout_line in stdout_lines:
        yield stdout_line

    popen.stdout.close()
    returncode =  popen.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, command)


def setup_experiment():
    iter = itertools.product((True, False), repeat=3)  # (cpu, network, io)
    confs = []
    for item in iter:
        confs.append(item)

    def run_next(_):
        if len(confs) > 0:
            conf = confs.pop()
            print "starting servers..."
            cmd = "python /home/laurens/PycharmProjects/untitled/benchmarks/Servers.py %s %s %s" % (NUM_CLIENTS, conf[0], conf[2])
            # print cmd
            # for p in execute(shlex.split(cmd)):
            #     print p
            p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
            print "waiting 120 sec for the servers to fire up"
            time.sleep(120)
            print "starting benchmark"
            b = Benchmark(conf)
            b.prepare_experiment()
            b.run().addBoth(lambda _ : p.kill()).addCallback(lambda _: time.sleep(5)).addCallback(run_next)
            # b.run().addCallback(run_next)
        else:
            reactor.stop()

    run_next(None)


if __name__ == "__main__":
    reactor.callWhenRunning(setup_experiment)
    reactor.run()
