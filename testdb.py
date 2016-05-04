from twisted.internet import reactor
from twisted.enterprise import adbapi
import os
def test():
    dbURL = os.path.join(os.getcwd(),"test.sqlite")
    print dbURL
    dbpool = adbapi.ConnectionPool("sqlite3" , dbURL)
    query = "SELECT * FROM Test"
    d = dbpool.runQuery(query)
    d.addCallback(printQuery)

def printQuery(result):
    print "QueryResult:" + str(result)

test()
reactor.run()