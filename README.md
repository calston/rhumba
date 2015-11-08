# Rhumba

[![Build Status](https://travis-ci.org/calston/rhumba.png?branch=master)](https://travis-ci.org/calston/rhumba)

Rhumba is an asynchronous job queue and distributed task service with scheduling.

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
