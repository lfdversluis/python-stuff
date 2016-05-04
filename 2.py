from twisted.internet import reactor, defer
from twisted.web.client import getPage
import re

@defer.inlineCallbacks
def lookup(country, search_term):
    try:
        query = "http://www.google.%s/search?q=%s" % (country,search_term)
        content = yield getPage(query)

        m = re.search('&lt;div id="?res.*?href="(?P&lt;url&gt;http://[^"]+)"',
                      content, re.DOTALL)
        if not m:
            defer.returnValue(None)

        url = m.group('url')
        content = yield getPage(url)

        m = re.search("<title>(.*?)</title>", content)
        if m:
            defer.returnValue(dict(url=url, title=m.group(1)))
        else:
            defer.returnValue(dict(url=url, title="{not-specified}"))

    except Exception, e:
        print ".%s FAILED: %s" % (country, str(e))

def printResult(result, country):
    if result:
        print ".%s result: %s (%s)" % (country, result['url'], result['title'])
    else:
        print ".%s result: nothing found" % country

def runme():
    all = []
    for country in ["com", "pl", "nonexistant"]:
        d = lookup(country, "Twisted")
        d.addCallback(printResult, country)
        all.append(d)
    defer.DeferredList(all).addCallback(lambda _: reactor.stop())

reactor.callLater(0, runme)
reactor.run()