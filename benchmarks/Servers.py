import gzip
import json
import os
import sqlite3
import sys
from io import BytesIO

from twisted.internet import reactor
from twisted.internet.defer import succeed
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.threads import deferToThread
from twisted.web import server, resource


class Server:
    INITIAL_PORT = 46527

    def __init__(self, i, io_blocking, cpu_blocking):
        self.num = i
        self.site = server.Site(SimpleAPI(i, io_blocking, cpu_blocking))
        db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db_server_%s" % self.num)
        self.conn = sqlite3.connect(db_filename, check_same_thread=False)
        cursor = self.conn.cursor()
        data_table_creation = """create table data (
                        indexString text,
                        squared INTEGER ,
                        cubed INTEGER);"""

        # Ensure to delete the old test table if it exists
        cursor.execute("DROP TABLE IF EXISTS data")
        cursor.execute(data_table_creation)
        cursor.close()
        self.conn.commit()
        self.conn.close()

        self.insert_data()

    def insert_data(self):
        # print "starting inserting..."
        db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db_server_%s" % self.num)
        self.conn = sqlite3.connect(db_filename, check_same_thread=False)
        cursor = self.conn.cursor()
        for i in xrange(1000000):
            cursor.execute("INSERT INTO data VALUES(?, ?, ?)", (str(i).rjust(6, '0'), i ** 2, i ** 3))
        cursor.close()
        self.conn.commit()
        self.conn.close()
        # print "done inserting."

    def create_server(self, port):
        ep = TCP4ServerEndpoint(reactor, port)

        def onStartedListening(port):
            self.port = port
            return "http://localhost:%s/test" % port.getHost().port

        return ep.listen(self.site).addCallback(onStartedListening)


class SimpleAPI(resource.Resource):
    def __init__(self, i, io_blocking, cpu_blocking):
        resource.Resource.__init__(self)

        self.event_request_handler = ZipRequestHandler(i, io_blocking, cpu_blocking)
        self.putChild("zip", self.event_request_handler)


class ZipRequestHandler(resource.Resource):
    isLeaf = True

    def __init__(self, i, io_blocking, cpu_blocking):
        resource.Resource.__init__(self)
        self.num = i
        self.io_blocking = io_blocking
        self.cpu_blocking = cpu_blocking

    def render_GET(self, request):
        i = request.args['i'][0]

        def on_data(data):
            json_deferred = self.data_to_json(data)
            json_deferred.addCallback(self.create_zip)
            json_deferred.addCallback(write_data)
            # create_zip_deferred = self.create_zip(json)
            # create_zip_deferred.addCallback(write_data)

        def write_data(data):
            request.write(data)
            request.finish()

        data_defer = self.query_data(i)
        data_defer.addCallback(on_data)

        return server.NOT_DONE_YET

    def data_to_json(self, data):

        def generate_json(data):
            number_list = []
            for i in xrange(len(data)):
                number_dict = {"squared": data[i][0],
                               "cubed": data[i][1]}
                number_list.append(number_dict)
            return json.dumps(number_list)

        if self.cpu_blocking:
            return succeed(generate_json(data))
        else:
            return deferToThread(generate_json, data)

    def query_data(self, i):
        """
        Queries data from a database, every index has 1000 rows of integers
        :param i: The index to query
        :return: A deferred that fires with the data
        """

        def retrieve_from_db(i):
            db_filename = os.path.join(os.getcwd(), "cpu_network_io_benchmark_db_server_%s" % self.num)
            self.conn = sqlite3.connect(db_filename, check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.execute("SELECT squared, cubed FROM data WHERE indexString LIKE ? ", (str(i) + "%",))
            data = cursor.fetchall()
            cursor.close()
            self.conn.commit()
            self.conn.close()
            return data

        if io_blocking:
            return succeed(retrieve_from_db(i))
        else:
            return deferToThread(retrieve_from_db, i)

    def create_zip(self, data):
        """
        Creates a zip in memory by using the BytesIO to represent a
        file-like object
        :param data: The data to be zipped
        :return: a Deferred that fires with a zipfile object when done.
        """

        def construct_zip(data):
            zip_data = BytesIO()
            g = gzip.GzipFile(fileobj=zip_data, mode='wb')
            g.write(data)
            g.close()
            # zipFile = zipfile.ZipFile(zip_data, "a", zipfile.ZIP_STORED, True)
            # zipFile.writestr('data.json', data)
            # zipFile.close()
            zip_data.seek(0)
            zipped_data = zip_data.read()
            zip_data.close()
            return zipped_data

        if self.cpu_blocking:
            return succeed(construct_zip(data))
        else:
            return deferToThread(construct_zip, data)


def start_servers(num_servers, io_blocking, cpu_blocking):
    for i in xrange(num_servers):
        s = Server(i, io_blocking, cpu_blocking)
        print s.create_server(s.INITIAL_PORT + i)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print "need to pass the amount of servers to start and two booleans: cpu_blocking and io_blocking"
    else:
        num_servs = int(sys.argv[1])
        cpu_blocking = sys.argv[2] == "True"
        io_blocking = sys.argv[3] == "True"
        reactor.callWhenRunning(start_servers, num_servs, io_blocking, cpu_blocking)
        reactor.run()
