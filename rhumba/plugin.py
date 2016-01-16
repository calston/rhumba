from rhumba.http_client import HTTPRequest

from twisted.python import log


class RhumbaPlugin(object):
    """
    Simple object for Rhumba plugins
    """
    def __init__(self, config, client):
        self.config = config
        self.client = client
        self.queue_name = config['name']
        self.http = HTTPRequest

    def log(self, *a, **kw):
        log.msg("[%s]: %s" % (self.config['name'], a[0]), *a[1:], **kw)

