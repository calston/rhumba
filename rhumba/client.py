import redis
import time
import json
import uuid

class RhumbaClient(object):
    """
    Rhumba client
    """
    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db

    def _get_client(self):
        return redis.StrictRedis(host=self.host, port=self.port, db=self.db)

    def queue(self, queue, message, params={}):
        """
        Queue a job in Rhumba
        """
        d = {
            'id': uuid.uuid1().get_hex(),
            'version': 1,
            'message': message,
            'params': params
        }

        self._get_client().lpush('rhumba.q.%s' % queue, json.dumps(d))
        print 'queued'
        return d['id']

    def getResult(self, queue, uid):
        """
        Retrieve the result of a job from its ID
        """
        return json.loads(
            self._get_client().get('rhumba.q.%s.%s' % (queue, uid))
        )

    def clusterStatus(self):
        """
        Returns a dict of cluster nodes and their status information
        """
        c = self._get_client()
        servers = c.keys('rhumba.server.*.heartbeat')
        
        d = {}

        now = time.time()

        for s in servers:
            sname = s.split('.', 2)[-1].rsplit('.', 1)[0]

            last = float(c.get('rhumba.server.%s.heartbeat' % sname))
            status = c.get('rhumba.server.%s.status' % sname)

            if (status == 'ready') and (now - last > 5):
                status = 'offline'

            d[sname] = {
                'lastseen': last,
                'status': status
            }

        return d
