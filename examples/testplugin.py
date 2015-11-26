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

        name = args.get('name')
        if name:
            defer.returnValue(["Hello, %s!" % name])
        else:
            defer.returnValue(["Hello!"])

    @cron(secs="*/5")
    def call_crontest(self, args):
        self.log("tick!")

