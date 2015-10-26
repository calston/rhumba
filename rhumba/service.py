import time
import json
import traceback
import yaml
import socket
import importlib

from twisted.application import service
from twisted.internet import task, reactor, protocol, defer
from twisted.python import log

from txredis.client import RedisClient, RedisSubscriber

class RhumbaQueue(object):
    def __init__(self, config, svc):
        self.config = config
        self.service = svc

        self.id = config['id']
        self.expire = int(config.get('expire', 3600))
        self.plugin = self.loadPlugin(config['plugin'])

        self.inter = float(config.get('inter', 1))

        self.fast_inter = float(config.get('fast_inter', 0.1))

        self.max_jobs = config.get('max_jobs', 5)
        self.jobs = 0
        self.cycl = 0

        self.t = task.LoopingCall(self.queueRun)

    def loadPlugin(self, plugin):
        return getattr(importlib.import_module(plugin), 'Plugin')(self.config)
        
    @defer.inlineCallbacks
    def grabQueue(self):
        item = yield self.service.client.rpop("rhumba.q%s" % self.id)
        if item:
            defer.returnValue(json.loads(item))
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def processQueue(self, item):
        m = item.get('message')
        if m:
            uid = item['id']
            yield self.service.setStatus("processing:%s:%s:%s" % (
                m, uid, time.time()))
            try:
                result = yield self.processItem(m, item.get('params', {}))

                d = {
                    'result': result,
                    'time': time.time()
                }

                yield self.service.client.set('rhumba.q%s.%s' % (self.id, uid),
                    json.dumps(d), expire=self.expire)

            except Exception, e:
                log.msg('Error %s' % e)
                log.msg(traceback.format_exc())

    @defer.inlineCallbacks
    def reQueue(self, request):
        response = yield self.service.client.lpush("rhumba.q%s" % self.id)

    def processItem(self, message, params):
        fn = getattr(self.plugin, 'call_%s' % message)

        return defer.maybeDeferred(fn, params)


    @defer.inlineCallbacks
    def queueRun(self):
        if self.cycl > 0:
            self.cycl -= 1
            defer.returnValue(None)

        if self.jobs >= self.max_jobs:
            defer.returnValue(None)
        
        item = yield self.grabQueue()

        if item:
            self.jobs += 1
            yield self.processQueue(item)
            self.jobs -= 1
        else:
            self.cycl = self.inter/self.fast_inter

            yield self.service.setStatus("ready")

        defer.returnValue(None)

    def startQueue(self):
        """Starts the timer for this queue"""
        self.td = self.t.start(self.fast_inter)

    def stopQueue(self):
        """Stops the timer for this queue"""
        self.td = None
        self.t.stop()


class RhumbaService(service.Service):
    """ Rhumba service
    Runs timers, configures sources and and manages the queue
    """
    def __init__(self, config):
        try:
            self.config = yaml.load(config)
        except:
            self.config = {}

        self.hostname = socket.gethostbyaddr(socket.gethostname())[0]

        self.redis_host = self.config.get('redis_host', 'localhost')
        self.redis_port = int(self.config.get('redis_port', 6379))

        self.expire = 3600
        
        self.queues = {}

        self.t = task.LoopingCall(self.heartbeat)

    def heartbeat(self):
        return self.client.set(
            "rhumba.server.%s.heartbeat" % self.hostname, time.time(), expire=self.expire)

    def setStatus(self, status):
        return self.client.set(
            "rhumba.server.%s.status" % self.hostname, status, expire=self.expire)

    def startBeat(self):
        self.td = self.t.start(1.0)

    def setupQueues(self):
        queues = self.config.get('queues', [])
        for queue in queues:
            self.queues[queue['name']] = RhumbaQueue(queue, self)

    @defer.inlineCallbacks
    def startService(self):
        clientCreator = protocol.ClientCreator(reactor, RedisClient)
        self.client = yield clientCreator.connectTCP(
            self.redis_host, self.redis_port)

        reactor.callWhenRunning(self.startBeat)
        self.setupQueues()
        for k, v in self.queues.items():
            reactor.callWhenRunning(v.startQueue)

def makeService(config):
    return RhumbaService(config)

