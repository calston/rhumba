import redis
import time
import json
import uuid

from twisted.internet import reactor, defer, protocol
from twisted.python import log

from rhumba.backend import RhumbaBackend

from txzookeeper.client import ZookeeperClient
from txzookeeper.queue import Queue, QueueItem

import zookeeper

class Backend(RhumbaBackend):
    """
    Rhumba redis backend
    """
    def __init__(self, config):
        self.config = config
        self.zk_url = self.config.get('zk_url', '127.0.0.1:2181')

        self.client = None

    @defer.inlineCallbacks
    def connect(self):
        client = ZookeeperClient(self.zk_url, 3000)
        log.msg('Connecting to %s' % self.zk_url)

        self.client = yield client.connect()

        yield self.setupPaths()

    @defer.inlineCallbacks
    def setupPaths(self):
        paths = ['/dq', '/q', '/dqr', '/qr', '/server', '/crons', '/qstats']
        for path in paths:
            yield self._try_create_node(path)

    @defer.inlineCallbacks
    def _try_create_node(self, path, recursive=True, *a, **kw):

        if recursive:
            try:
                yield self.client.create('/rhumba')
            except zookeeper.NodeExistsException:
                pass
                
            nodes = path.split('/')[1:]
            rpath = '/rhumba'
            for node in nodes:
                rpath = '/'.join([rpath, node])
                try:
                    if rpath == '/rhumba'+path:
                        yield self.client.create(rpath, *a, **kw)
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
        yield self._try_create_node(path)
        item = yield Queue('/rhumba'+path, self.client).get()

        defer.returnValue(item)

    @defer.inlineCallbacks
    def _get_key(self, path):
        yield self._try_create_node(path)
        item = yield self.client.get('/rhumba'+path)
        defer.returnValue(item[0])

    @defer.inlineCallbacks
    def _set_key(self, path, value, **kw):
        yield self._try_create_node(path, **kw)
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
            str(now))

    def registerCron(self, queue, uuid):
        return self._set_key('/crons/%s' % queue, uuid,
            flags=zookeeper.EPHEMERAL)

    def deregisterCron(self, queue):
        return self.client.delete('/rhumba/crons/%s' % queue)

    @defer.inlineCallbacks
    def checkCron(self, queue):
        path = '/crons/%s' % queue
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
            
            uid = yield self._get_key('/crons/%s' % queue)
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

