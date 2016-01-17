# Rhumba

[![Build Status](https://travis-ci.org/calston/rhumba.png?branch=master)](https://travis-ci.org/calston/rhumba)

Rhumba is an asynchronous job queue and distributed task service with scheduling.

## Installation 

```console
$ pip install rhumba
```

## Usage

Rhumba tasks are created as plugins configured on each Rhumba worker. The
plugin must contain a class called `Plugin` and callable task methods should be
prefixed with `call_`

```python
from rhumba import RhumbaPlugin

class Plugin(RhumbaPlugin):
    def call_test(self, args):
        self.log("Test call %s" % repr(args))
```

A plugin is connected to a queue in the Rhumba YAML configuration file

```yaml
queues:
    - name: myqueue
      plugin: myplugin
```

*Plugins must exist in the `PYTHON_PATH` of the process*

Then start up your Rhumba worker with `twistd -n rhumba -c myconfig.yaml`

```console
(ve)colin@nemesis:~/rhumba$ twistd -n rhumba -c examples/rhumba.yml
2015-11-08 23:15:20+0200 [-] Log opened.
2015-11-08 23:15:20+0200 [-] twistd 15.4.0 (/home/colin/rhumba/ve/bin/python 2.7.6) starting up.
2015-11-08 23:15:20+0200 [-] reactor class: twisted.internet.epollreactor.EPollReactor.
2015-11-08 23:15:20+0200 [-] Starting Rhumba
2015-11-08 23:15:20+0200 [-] Starting queue testqueue: plugin=<examples.testplugin.Plugin object at 0x7f0836655190>
2015-11-08 23:15:21+0200 [RedisClient,client] Queing testqueue scheduled job {'message': 'crontest', 'version': 1, 'params': {}, 'id': 'd1f23d42865d11e58d6482576555349e'}
2015-11-08 23:15:21+0200 [RedisClient,client] [testqueue]: tick!
```

Task methods can return a deferred, do _not_ implement blocking tasks.

```python
from twisted.mail.smtp import sendmail
from rhumba import RhumbaPlugin

class Plugin(RhumbaPlugin):
    def mailSent(self, from, to, msg):
        self.log('Mail from %s to %s sent successfully' % (from, to))

    def call_sendemail(self, args):
        a = (args['from'], args['to'], args['message'])
        d = sendmail('localhost', *a)

        d.addCallback(self.mailSent, *a)
        
        return d
```

## Scheduling tasks

Rhumbas `@cron` decorator can be used to schedule tasks.

```python
from rhumba import RhumbaPlugin, cron

class Plugin(RhumbaPlugin):
    @cron(hour=12)
    def call_scheduled(self, args):
        self.log("It's 12pm!")
```

The cron decorator accepts 5 keyword arguments: `secs`, `min`, `hour`, `day`,
`month` and `weekday`. All of these arguments except `weekday` can also accept
a string of the format `"*/n"` in which case the task will be run every `n`
seconds, minutes, hours, days or months. The arguments may contain any
combination of requirements as long as they make sense.

For example you may have a task run every 5 minutes from 1pm to 2pm on Sundays
in June. 

```python
from rhumba import RhumbaPlugin, cron, crontab

class Plugin(RhumbaPlugin):
    @cron(mins="*/5", hour=13, month=6, weekday=crontab.Sunday)
    def call_scheduled(self, args):
        self.log("It's 12pm!")
```

Matching ranges and lists is also possible, eg `hour="1-3"` or `day=(5, 6, 9)`
and any combination of these.

```python
from rhumba import RhumbaPlugin, cron, crontab

class Plugin(RhumbaPlugin):
    @cron(hour="8-17", weekday=crontab.Weekdays)
    def call_during_business_hours(self, args):
        # do stuff 
```

Tasks can also return JSON serialisable results.

## Calling tasks

Rhumba provides a synchronous client suitable for use with frameworks like
Django as an alternative to Celery, as well as an async client for Twisted.

### Synchronous client example

```python
from django.shortcuts import redirect
# etc... 

from rhumba.client import RhumbaClient

def sendemail(request):
    if request.method == "POST":
        form = SomeEmailForm(request.POST)
        if form.is_valid():
            c = RhumbaClient()
            c.queue('myqueue', 'sendemail', form.cleaned_data)

    return redirect('index')
```

You can *not* send Django ORM objects down the wire to Rhumba, if you need
to access your database you have to [reimplement those queries in Twisted](
https://twistedmatrix.com/documents/14.0.0/core/howto/rdbms.html)

You can retrieve the result of a task (if one is returned) until it expires
using the `getResult` method. Expiry can be set in the workers queue
configuration using the `expire` option and defaults to 1 hour.

```python
def sendemail(request):
    if request.method == "POST":
        form = SomeEmailForm(request.POST)
        if form.is_valid():
            c = RhumbaClient()
            taskid = c.queue('myqueue', 'sendemail', form.cleaned_data)

    return redirect('checkmail', id=taskid)

def checkmail(request, id):
    # This is a really silly example, do something sane instead...
    r = RhumbaClient().getResult('myqueue', id)
    if r:
        return redirect('index')
    else:
        return redirect('checkmail', id=id)

```

The async client rhumba.client.AsyncRhumbaClient provides the same API but
returns deferreds as expected using the txredis client.

## HTTP API

As with all new fangled software Rhumba provides an HTTP API on port 7701
which will return stats about queues and workers in the pool and also provides
a convenient way to queue tasks and wait for them without connecting directly
to the backend.

### Cluster stats

#### GET /cluster/
```json
{
    "workers": {
        "cthulhu": [
            {
                "status": "ready",
                "lastseen": 1448567076.76,
                "id": "ffc0dc9e947511e59f38448a5b5fd8c0"
            }
        ]
    },
    "queues": {
        "testqueue": {
            "waiting": 0, 
            "messages": {
                "crontest": {
                    "count": 345, 
                    "time": 22.99
                },
                "test": {
                    "count": 6,
                    "time": 52000.62
                }
            }
        }
    },
    "crons": {
        "testqueue": {
            "master": "ffc0dc9e947511e59f38448a5b5fd8c0:cthulhu",
            "methods": {
                "call_crontest": 1448567073.0
            }
        }
    }
}
```

### Queues

#### GET /queues/
```json
["testqueue"]
```

#### GET /queues/testqueue/call/test
```json
{
    "uid": "15b81fb6947711e58154448a5b5fd8c0"
}
```

#### GET /queues/testqueue/result/15b81fb6947711e58154448a5b5fd8c0
```json
{
    "result": ["Hello!"], 
    "time": 1448567500.198432
}
```

#### GET /queues/testqueue/wait/test
```json
{
    "result": ["Hello!"], 
    "time": 1448567475.792436
}

```

Both wait and call accept HTTP POST requests with a json payload

#### POST /queues/testqueue/wait/test {"name": "World"}
```json
{
    "result": ["Hello, World!"], 
    "time": 1448567851.689572
}
```

#### POST /queues/testqueue/fanout/test {"name": "World"}
```json
{
    "uid": "15b81fb6947711e58154448a5b5fd8c0"
}
```

Makes a fanout request to all servers active on a specific queue

#### POST /queues/testqueue/fanout/wait/test {"name": "World"}
```json
{
    "511c043cbcf411e5abaa82576555349e": {
        "result": ["Hello, test!"],
        "time": 1453019709.481646
    }, 
    "f363fe66bcf411e597fa448a5b5fd8c0": {
        "result": ["Hello, test!"],
        "time": 1453019597.918752
    }
}
```

Makes a fanout request to all servers and waits for all of them to return
a result
