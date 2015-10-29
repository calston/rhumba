import time
import uuid
import exceptions
import json
import traceback
import yaml
import socket
import importlib
import datetime

from twisted.application import service
from twisted.internet import task, reactor, protocol, defer
from twisted.python import log

from txredis.client import RedisClient, RedisSubscriber


class RhumbaQueue(object):
    def __init__(self, config, svc):
        self.config = config
        self.service = svc

        self.name = config['name']
        self.expire = int(config.get('expire', 3600))
        self.plugin = self.loadPlugin(config['plugin'])

        self.inter = float(config.get('inter', 1))

        self.fast_inter = float(config.get('fast_inter', 0.1))

        self.max_jobs = config.get('max_jobs', 5)
        self.jobs = 0
        self.cycl = 0

        self.t = task.LoopingCall(self.tick)

    def loadPlugin(self, plugin):
        try:
            return getattr(importlib.import_module(plugin), 'Plugin')(self.config)
        except exceptions.ImportError:
            log.msg("Error importing plugin %s" % plugin)
            return None
        
    @defer.inlineCallbacks
    def grabQueue(self):
        item = yield self.service.client.rpop("rhumba.q.%s" % self.name)
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

                yield self.service.client.set('rhumba.q.%s.%s' % (self.name, uid),
                    json.dumps(d), expire=self.expire)

            except Exception, e:
                log.msg('Error %s' % e)
                log.msg(traceback.format_exc())

    @defer.inlineCallbacks
    def reQueue(self, request):
        response = yield self.service.client.lpush("rhumba.q.%s" % self.name)

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

    def tick(self):
        reactor.callLater(self.fast_inter, self.queueRun)

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

        self.uuid = uuid.uuid1().get_hex()

        self.hostname = socket.gethostbyaddr(socket.gethostname())[0]

        self.redis_host = self.config.get('redis_host', 'localhost')
        self.redis_port = int(self.config.get('redis_port', 6379))

        self.expire = 3600
        
        self.queues = {}

        self.crons = {}

        self.t = task.LoopingCall(self.heartbeat)

    @defer.inlineCallbacks
    def checkCrons(self, now):

        for queue, crons in self.crons.items():
            runner = yield self.checkCronRunners(queue)

            if not runner:
                yield self.registerCronRunner(queue)
                runner = self.uuid

            if runner == self.uuid:
                for cron in crons:
                    lastRun = yield self.lastRun(queue, cron.name)

                    if lastRun:
                        lastRun = int(lastRun)

                    if cron.checkCron(lastRun, now):
                        plug = self.queues[queue].plugin
                        yield self.setLastRun(queue, cron.name, now)
                        d = {
                            'id': uuid.uuid1().get_hex(),
                            'version': 1,
                            'message': cron.name.split('call_', 1)[-1],
                            'params': {}
                        }

                        # Queue this job
                        log.msg('Queing %s scheduled job %s' % (queue, repr(d)))
                        yield self.client.lpush(
                            'rhumba.q.%s' % queue, json.dumps(d))
   
    @defer.inlineCallbacks
    def heartbeat(self):

        yield self.checkCrons(datetime.datetime.now())

        yield self.client.set(
            "rhumba.server.%s.heartbeat" % self.hostname, time.time(), expire=self.expire)

    def setStatus(self, status):
        return self.client.set(
            "rhumba.server.%s.status" % self.hostname, status, expire=self.expire)

    def startBeat(self):
        self.td = self.t.start(1.0)

    def lastRun(self, queue, fn):
        return self.client.get("rhumba.crons.%s.%s" % (queue, fn))

    def setLastRun(self, queue, fn, now):
        now = time.mktime(now.timetuple())
        return self.client.set("rhumba.crons.%s.%s" % (queue, fn), now)

    def registerCronRunner(self, queue):
        return self.client.set("rhumba.crons.%s" % queue, self.uuid, expire=60)

    def deregisterCronRunner(self, queue):
        return self.client.delete("rhumba.crons.%s" % queue)

    def checkCronRunners(self, queue):
        return self.client.get("rhumba.crons.%s" % queue)

    def setupQueues(self):
        queues = self.config.get('queues', [])
        for queue in queues:
            q = RhumbaQueue(queue, self)
            qname = queue['name']
            self.queues[qname] = q

            if q.plugin:
                # Find all methods decorated with @cron
                crons = [
                    getattr(q.plugin, i).cronable for i in dir(q.plugin)
                    if hasattr(getattr(q.plugin, i), 'cronable')
                ]

                if crons:
                    self.crons[qname] = crons

    @defer.inlineCallbacks
    def startService(self):
        clientCreator = protocol.ClientCreator(reactor, RedisClient)
        self.client = yield clientCreator.connectTCP(
            self.redis_host, self.redis_port)
        
        log.msg('Starting Rhumba')
    
        reactor.callWhenRunning(self.startBeat)
        self.setupQueues()
        queues = 0
        for k, v in self.queues.items():
            if v.plugin:
                queues += 1
                log.msg('Starting queue %s: plugin=%s' % (k, v.plugin))
                reactor.callWhenRunning(v.startQueue)

        if queues < 1:
            log.msg('No queues are running')

def makeService(config):
    return RhumbaService(config)

