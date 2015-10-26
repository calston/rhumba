from rhumba.client import RhumbaClient

rc = RhumbaClient()

rc.queue('testqueue', 'test', {})
