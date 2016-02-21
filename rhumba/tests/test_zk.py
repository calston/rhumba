import yaml
import json
import datetime

from twisted.trial import unittest

from twisted.internet import defer, reactor

from rhumba import service, client
from rhumba.backends import zk


from .plugin import sleep

test_config = {
    'noexpire': True,
    'backend': 'rhumba.backends.zk',
    'queues': [{
        'id': 0, 'max_jobs': 1,
        'name': 'testqueue',
        'plugin': 'rhumba.tests.plugin'
    }],
}

class Test(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.service = service.RhumbaService(yaml.dump(test_config))

        #backend = zookeeper.Backend(self.service.config)
        #self.service.client = backend

        yield self.service.startBackend()

        yield self.service.client.cleanNodes('/rhumba/q')
        yield self.service.client.cleanNodes('/rhumba/dq')
        yield self.service.client.cleanNodes('/rhumba/crons')
        yield self.service.client.cleanNodes('/rhumba/croner')

        yield self.service.client.setupPaths()
        self.service.setupQueues()

    @defer.inlineCallbacks
    def test_call(self):
        queue = self.service.queues['testqueue']

        uuid1 = yield self.service.client.queue('testqueue', 'test', {'count': 1, 'delay': 2})

        yield sleep(0.1)

        yield queue.queueRun()

        item = yield self.service.client.getResult('testqueue', uuid1)

        self.assertEquals(item['result'], None)

    @defer.inlineCallbacks
    def test_status(self):
        yield self.service.setStatus('test123')

        st = yield self.service.getStatus()

        self.assertEquals(st, 'test123')

    @defer.inlineCallbacks
    def test_fanout(self):
        queue = self.service.queues['testqueue']

        suid = self.service.uuid

        uuid1 = yield self.service.client.queue('testqueue', 'test', {'count': 1, 'delay': 1},
            uids=[suid, 'justsomefakeuuiddoesntmatter'])

        yield queue.queueFan()

        result = yield self.service.client.getResult('testqueue', uuid1, suid)

        self.assertEquals(result['result'], None)

        self.service.uuid = 'justsomefakeuuiddoesntmatter'
        yield queue.queueFan()

        result = yield self.service.client.getResult('testqueue', uuid1,
            self.service.uuid)

        self.assertEquals(result['result'], None)

    @defer.inlineCallbacks
    def _get_messages(self):
        try:
            nodes = yield self.service.client.client.get_children('/rhumba/q/testqueue')
        except:
            nodes = []
        queue = []
        for node in nodes:
            item = yield self.service.client.client.get('/rhumba/q/testqueue/%s' % node)
            queue.append(json.loads(item[0])['message'])
        defer.returnValue(queue)

    @defer.inlineCallbacks
    def test_cron(self):
        queue = self.service.queues['testqueue']

        yield self.service.checkCrons(datetime.datetime(2015, 3, 3, 5, 3, 0))

        item = yield self.service.client._get_key('/crons/testqueue/call_everysecond')

        self.assertEquals(type(float(item)), float)

        queue = yield self._get_messages()

        self.assertIn('everysecond', queue)

    @defer.inlineCallbacks
    def test_stats(self):
        queue = self.service.queues['testqueue']

        yield self.service.heartbeat()

        yield queue.queueRun()

        stats = yield self.service.client.getQueueMessageStats('testqueue')

        servers = yield self.service.client.getClusterServers()

        self.assertIn(self.service.hostname, servers)

        s = yield self.service.client.clusterQueues()

        self.assertEquals(s['testqueue'][0]['host'], self.service.hostname)

        s = yield self.service.client.clusterStatus()

        self.assertIn('testqueue', s['queues'].keys())
        self.assertIn(self.service.hostname, s['workers'].keys())
