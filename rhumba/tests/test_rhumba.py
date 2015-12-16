import yaml
import json
import datetime

from twisted.trial import unittest

from twisted.internet import defer, reactor

from rhumba import service, client
from rhumba.backends import redis

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

    def llen(self, key):
        return len(self.kv[key])

    def incr(self, *a):
        pass

    def rpop(self, key):
        if (key in self.kv) and self.kv[key]:
            return self.kv[key].pop(-1)
        else:
            return None

class AsyncWrapper(object):
    def __init__(self, c):
        self.c = c

    def incr(self, *a):
        return defer.maybeDeferred(self.c.incr, *a)

    def llen(self, *a):
        return defer.maybeDeferred(self.c.llen, *a)

    def lpush(self, *a, **kw):
        return defer.maybeDeferred(self.c.lpush, *a)

    def rpop(self, *a, **kw):
        return defer.maybeDeferred(self.c.rpop, *a)

    def set(self, *a, **kw):
        return defer.maybeDeferred(self.c.set, *a)

    def get(self, *a, **kw):
        return defer.maybeDeferred(self.c.get, *a)


class RhumbaTest(unittest.TestCase):
    def setUp(self):
        self.service = service.RhumbaService(yaml.dump(test_config))

        r = FakeRedis()

        self.service.setupQueues()

        redis_backend = redis.Backend(self.service.config)
        redis_backend.client = AsyncWrapper(r)
        self.service.client = redis_backend
        self.client = r

        self.c = client.RhumbaClient()
        self.c._get_client = lambda : r

class TestClient(client.AsyncRhumbaClient):
    def __init__(self, client, *a, **kw):
        self.client = client

    def connect(self):
        pass

class TestService(RhumbaTest):
    @defer.inlineCallbacks
    def test_async_client(self):
        rc = self.service.client.client
        client = TestClient(rc)

        yield client.queue('testqueue', 'test')
        
        message = rc.c.kv['rhumba.q.testqueue'][0]

        self.assertIn('test', message)

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

class TestCron(RhumbaTest):
    def _messages(self):
        """ Retrieve a list of only messages in the queue """
        q = [json.loads(i)['message'] for i in self.client.kv.get(
            'rhumba.q.testqueue', [])]
        return q

    def _flush(self):
        self.client.kv['rhumba.q.testqueue'] = []

    @defer.inlineCallbacks
    def test_cron(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 0))
        self.assertIn('rhumba.crons.testqueue.call_everysecond', self.client.kv)
        self.assertIn('everysecond', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 1))
        self.assertIn('everysecond', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 2))
        self.assertIn('everysecond', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 4))
        self.assertIn('everysecond', self._messages())
        self._flush()

    @defer.inlineCallbacks
    def test_cron_repeat(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime.now())
        self.assertIn('everysecond', self._messages())

    @defer.inlineCallbacks
    def test_cron_backwards_clock(self):
        queue = self.service.queues['testqueue']
        # Clock goes backwards
        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 0))
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 2, 3, 5, 3, 2))
        self.assertNotIn('everytwentythsecond', self._messages())

        yield self.service.checkCrons(datetime.datetime(2015, 1, 3, 5, 3, 20))
        self.assertIn('everytwentythsecond', self._messages())


    @defer.inlineCallbacks
    def test_cron_seconds(self):
        queue = self.service.queues['testqueue']

        now = datetime.datetime(2015, 3, 3, 4, 1, 1)

        yield self.service.checkCrons(now)
        self.assertNotIn('everytwentythsecond', self._messages())
        self._flush()

        # 20 second mark
        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 4, 1, 20))
        self.assertIn('everytwentythsecond', self._messages())
        self._flush()
        
        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 4, 1, 21))
        self.assertNotIn('everytwentythsecond', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 4, 1, 32))
        self.assertNotIn('everytwentythsecond', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 4, 3, 20))
        self.assertIn('everytwentythsecond', self._messages())

    @defer.inlineCallbacks
    def test_cron_mins(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 4, 2, 1))
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 4, 5, 1))
        self.assertNotIn('everytenminutes', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 4, 12, 1))
        self.assertIn('everytenminutes', self._messages())

    @defer.inlineCallbacks
    def test_cron_hour(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 10, 2, 1))
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 11, 2, 2))
        self.assertIn('everyhour', self._messages())
        self.assertNotIn('atlunch', self._messages())

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 12, 12, 1))
        self.assertIn('everyhour', self._messages())
        self.assertIn('atlunch', self._messages())

    @defer.inlineCallbacks
    def test_cron_mins_hour(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 10, 2, 1))
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 14, 0, 0))
        self.assertIn('everytenminutesat2pm', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 14, 12, 0))
        self.assertIn('everytenminutesat2pm', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 14, 14, 0))
        self.assertNotIn('everytenminutesat2pm', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 15, 14, 0))
        self.assertNotIn('everytenminutesat2pm', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 15, 25, 0))
        self.assertNotIn('everytenminutesat2pm', self._messages())
        self._flush()

    @defer.inlineCallbacks
    def test_cron_multi(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 10, 2, 1))
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 6, 10, 30, 0))
        self.assertIn('everyhour', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 6, 12, 32, 1))
        self.assertIn('everytwohoursontuesday', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 6, 12, 33, 1))
        self.assertNotIn('everytwohoursontuesday', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 6, 14, 34, 1))
        self.assertIn('everytwohoursontuesday', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 6, 14, 30, 20))
        self.assertNotIn('everytwohoursontuesday', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 7, 14, 12, 1))
        self.assertNotIn('everytwohoursontuesday', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 10, 7, 14, 30, 1))
        self.assertNotIn('everytwohoursontuesday', self._messages())
        self._flush()

    @defer.inlineCallbacks
    def test_cron_weekdays(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 12, 15, 10, 2, 1))
        self.assertIn('weekdays', self._messages())
        self.assertIn('businesshours', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 15, 11, 2, 1))
        self.assertNotIn('weekdays', self._messages())
        self.assertIn('businesshours', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 16, 11, 2, 2))
        self.assertIn('weekdays', self._messages())
        self.assertIn('businesshours', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 16, 11, 59, 59))
        self.assertNotIn('businesshours', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 16, 20, 1, 1))
        self.assertNotIn('businesshours', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 19, 11, 2, 2))
        self.assertNotIn('weekdays', self._messages())
        self.assertNotIn('businesshours', self._messages())
        self._flush()


    @defer.inlineCallbacks
    def test_cron_month(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 4, 3, 10, 2, 1))

        self.assertNotIn('december', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 1, 0, 0, 0))
        self.assertIn('december', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 2, 0, 0, 0))
        self.assertNotIn('december', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2015, 12, 31, 23, 59, 59))
        self.assertNotIn('december', self._messages())
        self._flush()

        yield self.service.checkCrons(datetime.datetime(2016, 12, 1, 0, 0, 0))
        self.assertIn('december', self._messages())
