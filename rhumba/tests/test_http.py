import json
import time

from twisted.trial import unittest

from twisted.internet import defer, reactor
from twisted.web import server, resource
from twisted.python import log

from rhumba import http_api
from rhumba.http_client import HTTPRequest

class TestResource(resource.Resource):
    isLeaf = True
    addSlash = True

    def completeCall(self, request):
        request.write("")
        try:
            request.finish()
        except:
            pass

    def render_GET(self, request):
        # Takes 5 seconds to complete
        reactor.callLater(2.5, self.completeCall, request)

        return server.NOT_DONE_YET

class RhumbaTest(unittest.TestCase):
    def sleep(self, secs):
        d = defer.Deferred()
        reactor.callLater(secs, d.callback, None)
        return d

    @defer.inlineCallbacks
    def setUp(self):
        site = server.Site(TestResource())
        self.con = yield reactor.listenTCP(1234, site)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.con.loseConnection()
        yield self.sleep(2.5)

    @defer.inlineCallbacks
    def test_client_timeout(self):
        t = time.time()
        try:
            d = yield HTTPRequest.getBody('http://localhost:1234/', timeout=1)
        except:
            pass

        l = time.time() - t

        self.assertLess(l, 2)
