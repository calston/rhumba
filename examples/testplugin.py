from twisted.internet import defer, reactor
from rhumba import RhumbaPlugin, cron

def sleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d

class Plugin(RhumbaPlugin):
    @defer.inlineCallbacks
    def call_test(self, args):
        self.log("Test call %s" % repr(args))
        yield sleep(2)

        defer.returnValue(None)

    @cron(1)
    def cron_test(self):
        self.log("tick!")

