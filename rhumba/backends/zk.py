# XXX Do not use this yet, txzookeeper is extremely unstable

import redis
import time
import json
import uuid
import cgi

from twisted.internet import reactor, defer, protocol, task
from twisted.python import log

from rhumba.backend import RhumbaBackend
from rhumba.http_client import HTTPRequest

from txzookeeper.client import ZookeeperClient
from txzookeeper.queue import Queue, QueueItem

import zookeeper


class Backend(RhumbaBackend):
    """
    Rhumba zookeeper backend
    """
    def __init__(self, config, parent):
        self.config = config
        self.zk_url = self.config.get('zk_url', '127.0.0.1:2181')

        self.parent = parent

        self.client = None

    @defer.inlineCallbacks
    def connect(self):
        client = ZookeeperClient(self.zk_url, 5000)
        log.msg('Connecting to %s' % self.zk_url)

        self.client = yield client.connect()
        
        if not self.config.get('noexpire', False):
            self.t = task.LoopingCall(self.expireResults)
            self.t.start(5.0)

        yield self.setupPaths()

    @defer.inlineCallbacks
    def close(self):
        if self.t:
            self.t.stop()
        yield self.client.close()

    @defer.inlineCallbacks
    def setupPaths(self):
        paths = ['/dq', '/q', '/dqr', '/qr', '/server', '/crons', '/qstats']
        for path in paths:
            yield self._try_create_node(path)

    @defer.inlineCallbacks
    def cleanNodes(self, path):
        try:
            it = yield self.client.get_children(path)
        except:
            it = []

        for i in it:
            yield self.cleanNodes(path + '/'+i)

        try:
            yield self.client.delete(path)
        except:
            pass

    @defer.inlineCallbacks
    def expireResults(self):
        queues = yield self.keys('/qr')
        for queue in queues:
            results = yield self.keys('/qr/%s' % queue)

            for result in results:
                path = '/rhumba/qr/%s/%s' % (queue, result)
                r = yield self.client.get(path)
                if r[0]:
                    dt = json.loads(r[0]).get('time')
                    delta = time.time() - dt
                    if delta > 60:
                        log.msg('Purging result %s/%s' % (queue, result))
                        yield self.client.delete(path)
                else:
                    log.msg('Purging result %s/%s' % (queue, result))
                    yield self.client.delete(path)

    @defer.inlineCallbacks
    def _try_create_node(self, path, recursive=True, flags=0):
        node = yield self.client.exists('/rhumba' + path)

        if node:
            defer.returnValue(None)

        if recursive:
            try:
                yield self.client.create('/rhumba')
            except zookeeper.NodeExistsException:
                pass
                
            nodes = path.split('/')[1:]
            rpath = '/rhumba'
            for node in nodes:
                rpath = '/'.join([rpath, node])
                node = yield self.client.exists(rpath)

                if not node:
                    try:
                        if rpath == '/rhumba'+path:
                            yield self.client.create(rpath, flags=flags)
                        else:
                            yield self.client.create(rpath)

                    except zookeeper.NodeExistsException:
                        pass
        else:
            try:
                yield self.client.create('/rhumba'+path, *a, **kw)
            except zookeeper.NodeExistsException:
                defer.returnValue(None)

    @defer.inlineCallbacks
    def _put_queue(self, path, item):
        yield self._try_create_node(path)
        put = yield Queue('/rhumba'+path, self.client, persistent=True
            ).put(item)

    @defer.inlineCallbacks
    def _get_queue(self, path):
        e = yield self.client.exists('/rhumba'+path)
        if e:
            item = yield Queue('/rhumba'+path, self.client).get()

            defer.returnValue(item)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def _get_key(self, path):
        e = yield self.client.exists('/rhumba'+path)
        if e:
            item = yield self.client.get('/rhumba'+path)
            defer.returnValue(item[0])
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def _set_key(self, path, value, flags=0):
        yield self._try_create_node(path, flags=flags)
        item = yield self.client.set('/rhumba'+path, value)

    @defer.inlineCallbacks
    def _inc_key(self, path, by=1):
        cl = yield self._get_key(path)

        if cl:
            new = int(cl) + by
        else:
            new = 0

        yield self._set_key(path, str(new))

    @defer.inlineCallbacks
    def queue(self, queue, message, params={}, uids=[]):
        """
        Queue a job in Rhumba
        """

        d = {
            'id': uuid.uuid1().get_hex(),
            'version': 1,
            'message': message,
            'params': params
        }

        ser = json.dumps(d)
        
        if uids:
            for uid in uids:
                path = '/dq/%s/%s' % (uid, queue)
                yield self._put_queue(path, ser)

        else:
            path = '/q/%s' % queue
            yield self._put_queue(path, ser)

        defer.returnValue(d['id'])

    @defer.inlineCallbacks
    def popQueue(self, queue):
        path = '/q/%s' % queue

        item = yield self._get_queue(path)

        if item:
            defer.returnValue(json.loads(item))
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def popDirectQueue(self, uid, queue):
        path = '/dq/%s/%s' % (uid, queue)

        item = yield self._get_queue(path)

        if item:
            defer.returnValue(json.loads(item))
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def getResult(self, queue, uid, suid=None):

        if suid:
            r = yield self._get_key('/dqr/%s/%s/%s' % (suid, queue, uid))
        else:
            r = yield self._get_key('/qr/%s/%s' % (queue, uid))

        if r:
            defer.returnValue(json.loads(r))
        else:
            defer.returnValue([])

    def waitForResult(self, queue, uid, timeout=3600, suid=None):
        d = defer.Deferred()

        t = time.time()

        def checkResult():
            def result(r):
                if r:
                    return d.callback(r)

                if (time.time() - t) > timeout:
                    raise Exception(
                        "Timeout waiting for result on %s:%s" % (queue, uid))
                else:
                    reactor.callLater(1, checkResult)

            self.getResult(queue, uid, suid=suid
                ).addCallback(result)

        reactor.callLater(0, checkResult)

        return d

    def setUUID(self, hostname, uuid, expire=None):
        return self._set_key('/server/%s/uuid' % hostname, uuid,
            flags=zookeeper.EPHEMERAL)

    def setHeartbeat(self, hostname, time, expire=None):
        return self._set_key('/server/%s/heartbeat' % hostname, str(time),
            flags=zookeeper.EPHEMERAL)

    def setQueues(self, hostname, jsond, expire=None):
        return self._set_key('/server/%s/queues' % hostname, jsond,
            flags=zookeeper.EPHEMERAL)

    def setStatus(self, hostname, status, expire=None):
        return self._set_key('/server/%s/status' % hostname,
            status, flags=zookeeper.EPHEMERAL)

    def getStatus(self, hostname):
        return self._get_key('/server/%s/status' % hostname)

    def setResult(self, queue, uid, result, expire=None, serverid=None):
        if serverid:
            return self._set_key(
                '/dqr/%s/%s/%s' % (serverid, queue, uid), result)
        else:
            return self._set_key('/qr/%s/%s' % (queue, uid), result)

    @defer.inlineCallbacks
    def getCron(self, queue, fn):
        path = '/crons/%s/%s' % (queue, fn)

        item = yield self._get_key(path)

        defer.returnValue(item)

    def setLastCronRun(self, queue, fn, now):
        return self._set_key('/crons/%s/%s' % (queue, fn),
            str(now), flags=zookeeper.EPHEMERAL)

    def registerCron(self, queue, uuid):
        return self._set_key('/croner/%s' % queue, uuid,
            flags=zookeeper.EPHEMERAL)

    def deregisterCron(self, queue):
        return self.cleanNodes('/rhumba/croner/%s' % queue)

    @defer.inlineCallbacks
    def checkCron(self, queue):
        path = '/croner/%s' % queue
        item = yield self._get_key(path)

        defer.returnValue(item)

    def keys(self, path):
        return self.client.get_children('/rhumba'+path)

    @defer.inlineCallbacks
    def queueSize(self, queue):
        items = yield self.client.get_children("/rhumba/q/%s" % queue)
        defer.returnValue(len(items))
    
    def delete(self, key):
        return self.client.delete(key)

    @defer.inlineCallbacks
    def queueStats(self, queue, message, duration):
        yield self._inc_key('/qstats/%s/%s/time' % (queue, message),
            int(duration*100000))

        yield self._inc_key('/qstats/%s/%s/count' % (queue, message))

    @defer.inlineCallbacks
    def getQueueMessageStats(self, queue):
        keys = yield self.keys('/qstats/%s' % queue)

        msgstats = {}
        for msg in keys:
            stats = yield self.keys('/qstats/%s/%s' % (queue, msg))

            for stat in stats:
                val = yield self._get_key(
                    '/qstats/%s/%s/%s' % (queue, msg, stat))

                if val:
                    if stat == 'time':
                        val = int(val)/100.0
                    else:
                        val = int(val)

                    if msg in msgstats:
                        msgstats[msg][stat] = val
                    else:
                        msgstats[msg] = {stat: val}

        defer.returnValue(msgstats)

    @defer.inlineCallbacks
    def getClusterServers(self):
        servers = yield self.keys('/server')
        
        snames = []
        for server in servers:
            beat = yield self._get_key('/server/%s/heartbeat' % server)
            if beat:
                if time.time() - float(beat) < 10:
                    snames.append(server)

        defer.returnValue(snames)

    @defer.inlineCallbacks
    def clusterQueues(self):
        """ Return a dict of queues in cluster and servers running them
        """
        servers = yield self.getClusterServers()

        queues = {}

        for sname in servers:
            qs = yield self._get_key('/server/%s/queues' % sname)
            uuid = yield self._get_key('/server/%s/uuid' % sname)
       
            qs = json.loads(qs)

            for q in qs:
                if q not in queues:
                    queues[q] = []

                queues[q].append({'host': sname, 'uuid': uuid})

        defer.returnValue(queues)

    @defer.inlineCallbacks
    def clusterStatus(self):
        """
        Returns a dict of cluster nodes and their status information
        """
        servers = yield self.getClusterServers()

        d = {
            'workers': {},
            'crons': {},
            'queues': {}
        }

        now = time.time()

        reverse_map = {}

        for sname in servers:
            last = yield self._get_key('/server/%s/heartbeat' % sname)
            status = yield self._get_key('/server/%s/status' % sname)
            uuid = yield self._get_key('/server/%s/uuid' % sname)

            reverse_map[uuid] = sname

            if not last:
                last = 0

            last = float(last)

            if (status == 'ready') and (now - last > 5):
                status = 'offline'

            if not sname in d['workers']:
                d['workers'][sname] = []

            d['workers'][sname].append({
                'lastseen': last,
                'status': status,
                'id': uuid
            })

        # Crons
        crons = yield self.keys('/crons')

        for queue in crons:
            if queue not in d['crons']:
                d['crons'][queue] = {'methods': {}}

            methods = yield self.keys('/crons/%s' % queue)

            for method in methods:
                last = yield self._get_key('/crons/%s/%s' % (queue, method))
                if last:
                    d['crons'][queue]['methods'][method] = float(last)
            
            uid = yield self._get_key('/croner/%s' % queue)
            if uid:
                d['crons'][queue]['master'] = '%s:%s' % (uid, reverse_map[uid])

        # Queues
        queue_keys = yield self.keys('/qstats')

        for qname in queue_keys:
            if qname not in d['queues']:
                qlen = yield self.queueSize(qname)

                stats = yield self.getQueueMessageStats(qname)

                d['queues'][qname] = {
                    'waiting': qlen,
                    'messages': stats
                }

        defer.returnValue(d)

