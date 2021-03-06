import json

from StringIO import StringIO

from zope.interface import implements

from twisted.internet import reactor, protocol, defer, error
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from twisted.web.client import Agent
from twisted.python import log

try:
    from twisted.internet.ssl import ClientContextFactory

    class WebClientContextFactory(ClientContextFactory):
        def getContext(self, hostname, port):
            return ClientContextFactory.getContext(self)
    SSL=True
except:
    SSL=False

try:
    from twisted.web import client
    client._HTTP11ClientFactory.noisy = False
    client.HTTPClientFactory.noisy = False
except:
    pass


class Timeout(Exception):
    """
    Raised to notify that an operation exceeded its timeout.
    """

class BodyReceiver(protocol.Protocol):
    """ Simple buffering consumer for body objects """
    def __init__(self, finished):
        self.finished = finished
        self.buffer = StringIO()

    def dataReceived(self, buffer):
        self.buffer.write(buffer)

    def connectionLost(self, reason):
        self.buffer.seek(0)
        self.finished.callback(self.buffer)

class StringProducer(object):
    """String producer for writing to HTTP requests
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

class HTTPRequest(object):
    def abort_request(self, request):
        """Called to abort request on timeout"""
        self.timedout = True
        try:
            request.cancel()
        except error.AlreadyCancelled:
            return

    @defer.inlineCallbacks
    def response(self, request):
        if request.length:
            d = defer.Deferred()
            request.deliverBody(BodyReceiver(d))
            b = yield d
            body = b.read()
        else:
            body = ""

        defer.returnValue(body)

    def request(self, url, method='GET', headers={}, data=None, socket=None, timeout=120):
        if socket:
            agent = SocketyAgent(reactor, socket)
        else:
            if url[:5] == 'https':
                if SSL:
                    agent = Agent(reactor, WebClientContextFactory())
                else:
                    raise Exception('HTTPS requested but not supported')
            else:
                agent = Agent(reactor)

        request = agent.request(method, url,
            Headers(headers),
            StringProducer(data) if data else None
        )

        if timeout:
            timer = reactor.callLater(timeout, self.abort_request,
                request)

            def timeoutProxy(request):
                if timer.active():
                    timer.cancel()
                return self.response(request)
            def requestAborted(failure):
                if timer.active():
                    timer.cancel()
    
                failure.trap(defer.CancelledError,
                             error.ConnectingCancelledError)
    
                raise Timeout(
                    "Request took longer than %s seconds" % timeout)

            request.addCallback(timeoutProxy).addErrback(requestAborted)
        else:
            request.addCallback(self.response)

        return request

    @classmethod
    def getBody(cls, url, method='GET', headers={}, data=None, socket=None, timeout=120):
        """Make an HTTP request and return the body
        """
        if not 'User-Agent' in headers:
            headers['User-Agent'] = ['Tensor HTTP checker']

        return cls().request(url, method, headers, data, socket, timeout)

    @classmethod
    @defer.inlineCallbacks
    def getJson(cls, url, method='GET', headers={}, data=None, socket=None, timeout=120):
        """Fetch a JSON result via HTTP
        """
        if not 'Content-Type' in headers:
            headers['Content-Type'] = ['application/json']

        body = yield cls().getBody(url, method, headers, data, socket, timeout)

        defer.returnValue(json.loads(body))

