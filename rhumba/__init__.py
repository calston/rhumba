from rhumba import plugin

class Cronable(object):
    def __init__(self, time, fn):
        self.time = time
        self.fn = fn
        self.name = fn.__name__

def cron(time):
    def inner(fn):
        return Cronable(time, fn)
    return inner

RhumbaPlugin = plugin.RhumbaPlugin
