from zope.interface import implements
 
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
 
from rhumba import service
 
class Options(usage.Options):
    optParameters = [
        ["config", "c", "rhumba.yml", "Config file"],
    ]
 
class RhumbaServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = "rhumba"
    description = "An asynchronous job queue"
    options = Options
 
    def makeService(self, options):
        try:
            config = open(options['config'])
        except:
            config = "{}"
        return service.makeService(config)
 
serviceMaker = RhumbaServiceMaker()
