"""
Test plugin for tests
"""
from twisted.internet import defer, reactor
from rhumba import RhumbaPlugin, cron, crontab

def sleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d

class Plugin(RhumbaPlugin):

    @defer.inlineCallbacks
    def call_test(self, args):
        self.log("Test call %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(secs='*/1')
    @defer.inlineCallbacks
    def call_everysecond(self, args):
        self.log("Test cron %s" % repr(args))
        print "test"
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(secs=20)
    @defer.inlineCallbacks
    def call_everytwentythsecond(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(min='*/10')
    @defer.inlineCallbacks
    def call_everytenminutes(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(min='*/10', hour=14)
    @defer.inlineCallbacks
    def call_everytenminutesat2pm(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(hour='*/1')
    @defer.inlineCallbacks
    def call_everyhour(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(month=12)
    @defer.inlineCallbacks
    def call_december(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(hour=12)
    @defer.inlineCallbacks
    def call_atlunch(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(weekday=crontab.Weekdays)
    def call_weekdays(self, args):
        self.log("Test cron %s" % repr(args))

    @cron(hour='8-17', weekday=crontab.Weekdays)
    def call_businesshours(self, args):
        self.log("Test cron %s" % repr(args))

    @cron(min='30', hour='*/2', weekday=crontab.Tuesday)
    @defer.inlineCallbacks
    def call_everytwohoursontuesday(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)

    @cron(min='30', hour='*/2', weekday=crontab.Tuesday, month='*/2')
    @defer.inlineCallbacks
    def call_every2hoursontuesdayevery2months(self, args):
        self.log("Test cron %s" % repr(args))
        yield sleep(args.get('delay', 1))
        defer.returnValue(None)
