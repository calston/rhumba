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
    def _set_key(self, path, value):
        yield self._try_create_node(path)
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
        yield self._try_create_node(path)
        item = yield Queue(path, self.client).get()

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
        return self._try_create_node('/server/%s/uuid' % hostname, data=uuid,
            flags=zookeeper.EPHEMERAL)

    def setHeartbeat(self, hostname, time, expire=None):
        return self._try_create_node('/server/%s/heartbeat' % hostname,
            data=str(time), flags=zookeeper.EPHEMERAL)

    def setQueues(self, hostname, jsond, expire=None):
        return self._try_create_node('/server/%s/queues' % hostname,
            data=jsond, flags=zookeeper.EPHEMERAL)

    def setStatus(self, hostname, status, expire=None):
        return self._try_create_node('/server/%s/status' % hostname,
            data=status, flags=zookeeper.EPHEMERAL)

    def setResult(self, queue, uid, result, expire=None, serverid=None):
        if serverid:
            return self._set_key(
                '/dqr/%s/%s/%s' % (serverid, queue, uid), result)
        else:
            return self._set_key('/qr/%s/%s' % (queue, uid), result)

    @defer.inlineCallbacks
    def getCron(self, queue, fn):
        path = '/crons/%s/%s' % (queue, fn)
        yield self._try_create_node(path)
        item = yield self.client.get(path)

        defer.returnValue(item)

    def setLastCronRun(self, queue, fn, now):
        return self._try_create_node('/crons/%s/%s' % (queue, fn),
            data=str(now))

    def registerCron(self, queue, uuid):
        return self._try_create_node('/crons/%s' % queue, data=uuid)

    def deregisterCron(self, queue):
        return self.client.delete('/rhumba/crons/%s' % queue)

    @defer.inlineCallbacks
    def checkCron(self, queue):
        path = '/crons/%s' % queue
        yield self._try_create_node(path)
        item = yield self.client.get(path)

        defer.returnValue(item)

    def keys(self, pattern):
        return self.client.keys(pattern)

    def queueSize(self, queue):
        return self.client.llen("rhumba.q.%s" % queue)
    
    def delete(self, key):
        return self.client.delete(key)

    @defer.inlineCallbacks
    def queueStats(self, queue, message, duration):
        yield self._inc_key('/qstats/%s/%s/time' % (queue, message),
            int(duration*100000))

        yield self._inc_key('/qstats/%s/%s/count' % (queue, message))

    @defer.inlineCallbacks
    def getQueueMessageStats(self, queue):
        keys = yield self.client.keys('rhumba.qstats.%s.*' % queue)
        msgstats = {}
        for key in keys:
            msg = key.split('.')[3]
            stat = key.split('.')[4]
            val = yield self.get(key)

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
        servers = yield self.keys('rhumba\.server\.*\.heartbeat')

        snames = [s.split('.', 2)[-1].rsplit('.', 1)[0] for s in servers]
        
        defer.returnValue(snames)

    @defer.inlineCallbacks
    def clusterQueues(self):
        """ Return a dict of queues in cluster and servers running them
        """
        servers = yield self.getClusterServers()

        queues = {}

        for sname in servers:
            qs = yield self.get('rhumba.server.%s.queues' % sname)
            uuid = yield self.get('rhumba.server.%s.uuid' % sname)
       
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
            last = yield self.get('rhumba.server.%s.heartbeat' % sname)
            status = yield self.get('rhumba.server.%s.status' % sname)
            uuid = yield self.get('rhumba.server.%s.uuid' % sname)

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
        crons = yield self.keys('rhumba\.crons\.*')

        for key in crons:
            segments = key.split('.')

            queue = segments[2]
            if queue not in d['crons']:
                d['crons'][queue] = {'methods': {}}

            if len(segments)==4:
                last = yield self.get(key)
                d['crons'][queue]['methods'][segments[3]] = float(last)
            else:
                uid = yield self.get(key)
                d['crons'][queue]['master'] = '%s:%s' % (uid, reverse_map[uid])

        # Queues
        queue_keys = yield self.keys('rhumba.qstats.*')

        for key in queue_keys:
            qname = key.split('.')[2]
            if qname not in d['queues']:
                qlen = yield self.queueSize(qname)

                stats = yield self.getQueueMessageStats(qname)

                d['queues'][qname] = {
                    'waiting': qlen,
                    'messages': stats
                }

        defer.returnValue(d)

