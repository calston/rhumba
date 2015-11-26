import redis
import time
import json
import uuid

from twisted.internet import reactor, defer, protocol
from twisted.python import log

from txredis.client import RedisClient

from rhumba.backend import RhumbaBackend

class Backend(RhumbaBackend):
    """
    Rhumba redis backend
    """
    def __init__(self, config):
        self.config = config
        self.redis_host = self.config.get('redis_host', 'localhost')
        self.redis_port = int(self.config.get('redis_port', 6379))
        self.redis_db = int(self.config.get('redis_db', 0))

        self.client = None

    @defer.inlineCallbacks
    def connect(self):
        clientCreator = protocol.ClientCreator(reactor, RedisClient, db=self.redis_db)

        self.client = yield clientCreator.connectTCP(
            self.redis_host, self.redis_port
        )

    @defer.inlineCallbacks
    def queue(self, queue, message, params={}):
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

        yield self.client.lpush('rhumba.q.%s' % queue, json.dumps(d))

        defer.returnValue(d['id'])

    @defer.inlineCallbacks
    def popQueue(self, queue):
        item = yield self.client.rpop("rhumba.q.%s" % queue)
        if item:
            defer.returnValue(json.loads(item))
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def getResult(self, queue, uid):
        r = yield self.client.get('rhumba.q.%s.%s' % (queue, uid))

        if r:
            defer.returnValue(json.loads(r))
        else:
            defer.returnValue([])

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
    def clusterStatus(self):
        """
        Returns a dict of cluster nodes and their status information
        """
        servers = yield self.keys('rhumba\.server\.*\.heartbeat')

        d = {
            'workers': {},
            'crons': {},
            'queues': {}
        }

        now = time.time()

        reverse_map = {}

        for s in servers:
            sname = s.split('.', 2)[-1].rsplit('.', 1)[0]

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

