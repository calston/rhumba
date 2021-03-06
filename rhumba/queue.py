import time
import exceptions
import json
import importlib

from twisted.internet import task, reactor, defer
from twisted.python import log


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
            return getattr(importlib.import_module(plugin), 'Plugin')(
                self.config, self.service.client)

        except exceptions.ImportError, e:
            log.msg("Error importing plugin %s : %s" % (plugin, repr(e)))
            return None
        
    @defer.inlineCallbacks
    def processQueue(self, item, fan=False):
        m = item.get('message')
        if m:
            uid = item['id']
            yield self.service.setStatus("processing:%s:%s:%s" % (
                m, uid, time.time()))

            start = time.time()
            result = yield self.processItem(m, item.get('params', {}))
            duration = time.time() - start

            d = {
                'result': result,
                'time': time.time()
            }

            yield self.service.client.queueStats(self.name, m, duration)

            result = json.dumps(d)
            
            if fan:
                yield self.service.client.setResult(self.name, uid, result,
                    serverid=self.service.uuid, expire=self.expire)

            else:
                yield self.service.client.setResult(self.name, uid, result,
                    expire=self.expire)

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

        item = yield self.service.client.popQueue(self.name)

        if item:
            self.jobs += 1
            yield self.processQueue(item)
            self.jobs -= 1
        else:
            self.cycl = self.inter/self.fast_inter

            yield self.service.setStatus("ready")

        defer.returnValue(None)

    @defer.inlineCallbacks
    def queueFan(self):

        item = yield self.service.client.popDirectQueue(self.service.uuid, self.name)

        if item:
            yield self.processQueue(item, fan=True)
        else:
            yield self.service.setStatus("ready")

        defer.returnValue(None)

    def tick(self):
        reactor.callLater(self.fast_inter, self.queueRun)
        reactor.callLater(self.fast_inter, self.queueFan)

    def startQueue(self):
        """Starts the timer for this queue"""
        self.td = self.t.start(self.fast_inter)

    def stopQueue(self):
        """Stops the timer for this queue"""
        self.td = None
        self.t.stop()
