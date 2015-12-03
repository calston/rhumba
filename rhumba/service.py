import time
import uuid
import exceptions
import importlib
import json
import yaml
import socket
import datetime

from twisted.application import service
from twisted.internet import task, reactor, protocol, defer
from twisted.python import log

from txredis.client import RedisClient

from rhumba.queue import RhumbaQueue
from rhumba.http_api import APIService

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

        self.backend = self.config.get('backend', 'rhumba.backends.redis')

        self.api_service = self.config.get('api_enabled', True)

        self.hostname = self.config.get('hostname')
        if not self.hostname:
            hs = socket.gethostname()
            if '.' in hs:
                self.hostname = hs
            else:
                # Try and resolve our FQDN
                try:
                    self.hostname = socket.gethostbyaddr(hs)[0]
                except:
                    self.hostname = hs

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

                    if cron.checkCron(lastRun, now):
                        plug = self.queues[queue].plugin
                        yield self.setLastRun(queue, cron.name, now)

                        # Queue this job
                        yield self.client.queue(queue, cron.name.split('call_', 1)[-1])
   
    @defer.inlineCallbacks
    def heartbeat(self):
        yield self.checkCrons(datetime.datetime.now())

        yield self.client.set(
            "rhumba.server.%s.uuid" % self.hostname, self.uuid, expire=self.expire)

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
    def stopService(self):
        for queue, crons in self.crons.items():
            runner = yield self.checkCronRunners(queue)

            if runner == self.uuid:
                log.msg('Deregistering queue %s' % queue)
                yield self.deregisterCronRunner(queue)

    @defer.inlineCallbacks
    def startBackend(self):
        try:
            self.client = getattr(importlib.import_module(
                self.backend), 'Backend')(self.config)
            yield self.client.connect()

        except exceptions.ImportError, e:
            raise Exception("Unable to load backend %s" % self.backend)

    @defer.inlineCallbacks
    def startService(self):
        yield self.startBackend()
        
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

        if self.api_service:
            self.api = APIService(self.config, self)

            reactor.callWhenRunning(self.api.startAPI)

def makeService(config):
    return RhumbaService(config)

