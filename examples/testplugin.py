from rhumba import RhumbaPlugin

class Plugin(RhumbaPlugin):
    def call_test(self, args):
        print "Test call", args
        pass
