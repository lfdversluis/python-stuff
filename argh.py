class Test():

    def __init__(self):
        self._pending_response_dict = {}
        self._pending_response_dict["2"] = {u'infohash': "2",
                                            u'remaining_responses': 1,
                                            u'seeders': -2,
                                            u'leechers': -2,
                                            u'updated': False}

        if self._pending_response_dict["2"][u"updated"]:
            print "ok"

t = Test()
