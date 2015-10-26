from twisted.python import log

class RhumbaPlugin(object):
    """
    Simple object for Rhumba plugins
    """
    def __init__(self, config):
        self.config = config

    def log(self, *a, **kw):
        log.msg("[%s]: %s" % (self.config['name'], a[0]), *a[1:], **kw)
