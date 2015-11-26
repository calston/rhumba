# -*- coding: utf-8 -*-
from twisted.web import server, resource
from twisted.internet import defer, reactor
from twisted.python import log

import json
import cgi
import hmac
import hashlib
import base64
import re

try:
    from urllib.parse import urlparse
except:
    from urlparse import urlparse

try:
    from twisted.internet import ssl

    SSL=True
except:
    SSL=False


class APIProcessor(object):
    def __init__(self, service, config):
        self.service = service
        self.config = config

    def queue_detail(self, request, queue):
        return {
            'name': queue
        }

    def queue_call(self, request, queue, method):
        params = request.data

        return self.service.queue(queue, method, params)

    def list_queues(self, request):
        log.msg('List queues' + repr(request))

        return self.service.queues.keys()

class APIResource(resource.Resource):
    isLeaf = True
    addSlash = True

    def __init__(self, config, service):
        self.config = config
        self.service = service
        self.rhumba_service = service.service

        api = APIProcessor(self.rhumba_service, self.config)

        self.paths = (
            (r'^/queues/(\w+)/call/(\w+)', api.queue_call),
            (r'^/queues/(\w+)', api.queue_detail),
            (r'^/queues/$', api.list_queues),
        )

    def completeCall(self, response, request):
        # Render the json response from call
        response = json.dumps(response)
        request.write(response)
        request.finish()

    def getProcessor(self, path):
        for regex, processor in self.paths:
            m = re.match(regex, path)
            if m:
                return m.groups(), processor
        
        return [], []

    def jsonRequest(self, path, request, data={}):
        params, proc = self.getProcessor(path)
        request.data = data
        if proc:
            return proc(request, *params)
        else:
            return None
        
    def getHeader(self, request, header, default=None):
        head = request.requestHeaders.getRawHeaders(header)
        if head:
            return head[0]
        else:
            return default

    def getSecret(self, auth):
        if auth == self.service.api_token:
            return self.service.api_secret

    def checkSignature(self, request, data=None):
        auth = self.getHeader(request, 'authorization', None)
        sig = self.getHeader(request, 'sig', None)

        if not (auth and sig):
            return False

        sign = [auth, request.method, request.path]
        if data:
            sign.append(
                hashlib.sha1(data).hexdigest()
            )

        key = self.getSecret(auth)

        if key:
            mysig = hmac.new(
                key=key,
                msg='\n'.join(sign),
                digestmod=hashlib.sha1
            ).digest()

            return base64.b64encode(mysig) == sig
        else:
            return False

    def render_GET(self, request):
        request.setHeader("content-type", "application/json")

        if self.service.api_token: 
            if not self.checkSignature(request):
                return '["Not authorized"]'

        d = defer.maybeDeferred(self.jsonRequest, request.path, request)

        d.addCallback(self.completeCall, request)

        return server.NOT_DONE_YET

    def render_POST(self, request):
        request.setHeader("content-type", "application/json")
        # Get request
        data = cgi.escape(request.content.read())

        if self.service.api_token: 
            if not self.checkSignature(request, data):
                return '["Not authorized"]'

        d = defer.maybeDeferred(self.jsonRequest, request.path, request,
            json.loads(data)
        )

        d.addCallback(self.completeCall, request)

        return server.NOT_DONE_YET

class APIService(object):
    def __init__(self, config, service):
        self.config = config
        self.service = service

        self.api_port = int(config.get('api_port', 7701))
        self.api_token = config.get('api_token', None)
        self.api_secret = config.get('api_secret', None)
        self.api_ssl = config.get('api_ssl', False)
        self.api_ssl_cert = config.get('api_ssl_cert', None)
        self.api_ssl_key = config.get('api_ssl_key', None)

    def startAPI(self):
        site = server.Site(APIResource(self.config, self))

        if self.api_ssl:
            if SSL:
                reactor.listenSSL(self.api_port, site,
                    ssl.DefaultOpenSSLContextFactory(
                        self.api_ssl_key, self.api_ssl_cert
                    )
                )
            else:
                raise Exception("Unable to start SSL API service, no OpenSSL")

        else:
            reactor.listenTCP(self.api_port, site)

