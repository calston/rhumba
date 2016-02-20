import yaml
import json
import datetime

from twisted.trial import unittest

from twisted.internet import defer, reactor

from rhumba import service, client
from rhumba.backends import zk

test_config = {
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
        self.service.setupQueues()

    def test_start_backend(self):
        pass
        

    @defer.inlineCallbacks
    def test_call(self):
        queue = self.service.queues['testqueue']

        uuid1 = yield self.service.client.queue('testqueue', 'test', {'count': 1, 'delay': 2})
        print "Queued..."

        yield queue.queueRun()

        #result = self.c.getResult('testqueue', uuid2)
