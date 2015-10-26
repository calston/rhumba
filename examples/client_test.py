import sys
sys.path.append('..')

from rhumba.client import RhumbaClient

rc = RhumbaClient()

for i in range(int(sys.argv[1])):
    rc.queue('testqueue', 'test', {'count': i})
