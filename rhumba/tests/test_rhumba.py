import yaml 

from twisted.trial import unittest

from twisted.internet import defer, reactor, error
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

from rhumba import service

test_config = {
    'redis_host': 'localhost',
    'queues': [{
        'id': 0, 'max_jobs': 1,
        'name': 'sideloader',
        'plugin': 'rhumba.tests.plugin'
    }],
    'redis_port': 6379
}

class FakeRedis(object):
    """Fake redis client object
    """
    def __init__(self):
        self.kv = {}

    def _lpush(self, key, val, expire=None):
        if key in self.kv:
            self.kv[key].append(val)
        else:
            self.kv[key] = [val]

    def _set(self, key, val, expire=None):
        self.kv[key] = val

    def _rpop(self, key, val):
        if (key in self.kv) and self.kv[key]:
            return self.kv[key].pop(0)
        else:
            return None

    def lpush(self, *a, **kw):
        return defer.maybeDeferred(self._lpush, *a)

    def rpop(self, *a, **kw):
        return defer.maybeDeferred(self._rpop, *a)

    def set(self, *a, **kw):
        return defer.maybeDeferred(self._set, *a)

class Tests(unittest.TestCase):
    def setUp(self):
        self.service = service.RhumbaService(yaml.dump(test_config))
        self.client = FakeRedis()
        self.service.setupQueues()
        self.service.client = self.client

    @defer.inlineCallbacks
    def test_heartbeat(self):
        yield self.service.heartbeat()

        hb = self.client.kv.get("rhumba.server.%s.heartbeat" % self.service.hostname)

        self.assertTrue(hb is not None)
    
    @defer.inlineCallbacks
    def test_status(self):
        yield self.service.setStatus('test')

        hb = self.client.kv.get("rhumba.server.%s.status" % self.service.hostname)
        self.assertEquals(hb, 'test')

    def test_queues(self):
        self.assertTrue('sideloader' in self.service.queues)

