import time

from twisted.internet import defer

class RhumbaBackend(object):
    def __init__(self, config):
        self.config = config

