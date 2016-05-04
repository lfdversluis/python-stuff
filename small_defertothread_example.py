import os

import sqlite3

import time
from twisted.internet import reactor

from twisted.internet.threads import deferToThread


class Example:

    def start_experiment(self):
        reactor.callWhenRunning(self.create_table)
        reactor.run()

    def create_table(self):
        db_filename = os.path.join(os.getcwd(), "small_test")
        self.conn = sqlite3.connect(db_filename, check_same_thread=False)
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
        for i in xrange(100000):
            cursor.execute("insert into data values(?, ?, ?)", (str(i).rjust(5, '0'), i**2, i**3))
        self.conn.commit()
        print "done inserting."
        self.sync()

    def sync(self):
        print "starting sync"
        t = time.time() * 1e3
        data = self.query_data(1)
        print "sync time: %s " % (time.time() * 1e3 - t)
        self.async()

    def async(self):
        print "starting async"
        t = time.time() * 1e3
        def on_data(data):
            print "async time: %s " % (time.time() * 1e3 - t)
            reactor.stop()

        deferred = deferToThread(self.query_data, 1)
        deferred.addCallback(on_data)

    def query_data(self, i):
        """
        Queries data from a database
        :param i: The index of which the indexString has to start with.
        :return: A list containing tuples of data.
        """
        print "HERE"
        cursor = self.conn.cursor()
        cursor.execute("SELECT squared, cubed FROM data WHERE indexString like ? ", (str(i)+"%",))
        print "RETURNING"
        return cursor.fetchall()

if __name__ == "__main__":
    b = Example()
    b.start_experiment()
