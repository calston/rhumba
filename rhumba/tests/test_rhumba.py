import yaml 

from twisted.trial import unittest

from twisted.internet import defer, reactor, error
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

from rhumba import service, client

test_config = {
    'redis_host': 'localhost',
    'queues': [{
        'id': 0, 'max_jobs': 1,
        'name': 'testqueue',
        'plugin': 'rhumba.tests.plugin'
    }],
    'redis_port': 6379
}

class FakeRedis(object):
    """Fake redis client object
    """
    def __init__(self):
        self.kv = {}

    def lpush(self, key, val, expire=None):
        if key in self.kv:
            self.kv[key].insert(0, val)
        else:
            self.kv[key] = [val]

    def set(self, key, val, expire=None):
        self.kv[key] = val

    def get(self, key):
        return self.kv.get(key, None)

    def rpop(self, key):
        if (key in self.kv) and self.kv[key]:
            return self.kv[key].pop(-1)
        else:
            return None

class AsyncWrapper(object):
    def __init__(self, c):
        self.c = c

    def lpush(self, *a, **kw):
        return defer.maybeDeferred(self.c.lpush, *a)

    def rpop(self, *a, **kw):
        return defer.maybeDeferred(self.c.rpop, *a)

    def set(self, *a, **kw):
        return defer.maybeDeferred(self.c.set, *a)

    def get(self, *a, **kw):
        return defer.maybeDeferred(self.c.get, *a)


class Tests(unittest.TestCase):
    def setUp(self):
        self.service = service.RhumbaService(yaml.dump(test_config))
        self.client = FakeRedis()
        self.service.setupQueues()
        self.service.client = AsyncWrapper(self.client)

        self.c = client.RhumbaClient()
        self.c._get_client = lambda : self.client

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
        self.assertTrue('testqueue' in self.service.queues)

    def _wait_for(self, queue, uid):
        d = defer.Deferred()
        def poll(d):
            result = self.c.getResult(queue, uid)

            if result:
                d.callback(result)
            else:
                reactor.callLater(0.1, poll, d)

        reactor.callLater(0.1, poll, d)
        return d

    @defer.inlineCallbacks
    def test_call(self):
        queue = self.service.queues['testqueue']

        uuid1 = self.c.queue('testqueue', 'test', {'count': 1, 'delay': 2})
        uuid2 = self.c.queue('testqueue', 'test', {'count': 2, 'delay': 1})

        reactor.callLater(0, queue.queueRun)
        reactor.callLater(1, queue.queueRun)

        result = yield self._wait_for('testqueue', uuid1)
        self.assertEquals(result['result'], None)

        result = self.c.getResult('testqueue', uuid2)
        self.assertEquals(result, None)

        reactor.callLater(0, queue.queueRun)

        result = yield self._wait_for('testqueue', uuid2)
        
        self.assertEquals(result['result'], None)

    @defer.inlineCallbacks
    def test_cron(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons()

        self.assertIn('rhumba.crons.testqueue.call_crontest', self.client.kv)

        q = yield queue.grabQueue()

        self.assertEquals(q['message'], 'crontest')

