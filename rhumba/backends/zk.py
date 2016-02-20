import redis
import time
import json
import uuid

from twisted.internet import reactor, defer, protocol
from twisted.python import log

from rhumba.backend import RhumbaBackend

from txzookeeper.client import ZookeeperClient
from txzookeeper.queue import Queue

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
        paths = ['/dq', '/q', '/server', '/crons', '/qstats']
        for path in paths:
            yield self._try_create_node(path)

    @defer.inlineCallbacks
    def _try_create_node(self, path, recursive=True):
        log.msg('Creating ZK node %s' % path)

        if recursive:
            try:
                yield self.client.create('/rhumba')
            except zookeeper.NodeExistsException:
                pass
                
            nodes = path.split('/')[1:]
            path = '/rhumba'
            for node in nodes:
                path = '/'.join([path, node])
                try:
                    yield self.client.create(path)
                except zookeeper.NodeExistsException:
                    pass
        else:
            try:
                yield self.client.create('/rhumba'+path)
            except zookeeper.NodeExistsException:
                defer.returnValue(None)

    @defer.inlineCallbacks
    def _put_queue(self, queue, item):
        path = '/q/%s' % queue
        yield self._try_create_node(path)
        yield Queue('/rhumba'+path, self.client).put(item)

    @defer.inlineCallbacks
    def _get_queue(self, queue):
        yield self._try_create_node(queue)
        item = yield Queue('/rhumba'+path, self.client).get()
        defer.returnValue(item)

    @defer.inlineCallbacks
    def queue(self, queue, message, params={}, uids=[]):
        """
        Queue a job in Rhumba
        """
        log.msg('Queing job %s/%s:%s' % (queue, message, repr(params)))

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
            r = yield self.client.get('rhumba.dq.%s.%s.%s' % (suid, queue, uid))
        else:
            r = yield self.client.get('rhumba.q.%s.%s' % (queue, uid))

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

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value, expire=None):
        return self.client.set(key, value, expire=expire)

    def keys(self, pattern):
        return self.client.keys(pattern)

    def queueSize(self, queue):
        return self.client.llen("rhumba.q.%s" % queue)
    
    def delete(self, key):
        return self.client.delete(key)

    @defer.inlineCallbacks
    def queueStats(self, queue, message, duration):
        yield self.client.incr(
            'rhumba.qstats.%s.%s.time' % (queue, message),
            int(duration*100000)
        )

        yield self.client.incr(
            'rhumba.qstats.%s.%s.count' % (queue, message))

    @defer.inlineCallbacks
    def getQueueMessageStats(self, queue):
        keys = yield self.client.keys('rhumba.qstats.%s.*' % queue)
        msgstats = {}
        for key in keys:
            msg = key.split('.')[3]
            stat = key.split('.')[4]
            val = yield self.get(key)

            print key, val
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

