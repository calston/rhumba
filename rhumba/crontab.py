import datetime
import calendar

Sunday = 0
Monday = 1
Tuesday = 2
Wednesday = 3
Thursday = 4
Friday = 5
Saturday = 6

Weekdays = (1, 2, 3, 4, 5)

class Cronable(object):
    def __init__(self, name, secs, min, hour, day, month, weekday):
        self.name = name
        self.secs = secs
        self.min = min
        self.hour = hour
        self.day = day
        self.month = month
        self.weekday = weekday

        self.chk = [
            (self.secs, self._checkSecs),
            (self.min, self._checkMins),
            (self.hour, self._checkHours),
            (self.day, self._checkDays),
            (self.weekday, self._checkWeekday),
            (self.month, self._checkMonth),
        ]

        self.ignore_delta = False
        for a, b in self.chk:
            if isinstance(a, str) and (a.startswith('*/')):
                self.ignore_delta = True

    def _checkEvery(self, t, n, delta, cur, wrap=59, exact=True):
        if not t:
            return False
        tseg = t

        if isinstance(t, str):
            if t.startswith('*/'):
                tseg = int(t[2:])
                if delta >= tseg:
                    return True
                return False
            elif '-' in t:
                ra_start, ra_stop = [int(i) for i in t.split('-')]
                tseg = range(ra_start, ra_stop+1)
            else:
                tseg = int(t)

        if (isinstance(tseg, list) or isinstance(tseg, tuple)):
            if (n in tseg) and ((delta >= 1) or self.ignore_delta or cur):
                return True

        delmatch = (delta >= wrap) or self.ignore_delta or cur

        if exact:
            if (tseg == n) and delmatch:
                return True

        else:
            if (n >= tseg) and delmatch:
                return True

        return False

    def _checkSecs(self, now, delta, cur):
        delta_secs = delta.total_seconds()

        return self._checkEvery(
            self.secs, now.second, delta_secs, cur, exact=False)

    def _checkMins(self, now, delta, cur):
        delta_mins = delta.total_seconds() / 60

        return self._checkEvery(
            self.min, now.minute, delta_mins, cur, exact=False)

    def _checkHours(self, now, delta, cur):
        delta_hours = delta.total_seconds() / (60*60)

        return self._checkEvery(self.hour, now.hour, delta_hours, cur, wrap=23)
    
    def _checkDays(self, now, delta, cur):
        delta_days = delta.total_seconds() / (60*60*24)

        # Get the max days in this month
        start, end = calendar.monthrange(now.year, now.month)

        return self._checkEvery(self.day, now.day, delta_days, cur, wrap=end)

    def _checkMonth(self, now, delta, cur):
        # Get the max days in this month
        if now.month == 1:
            lastm = 12
            year = now.year - 1
        else:
            lastm = now.month - 1
            year = now.year

        start, end = calendar.monthrange(year, lastm)

        delta_days = delta.total_seconds() / (60*60*24*end)

        return self._checkEvery(self.month, now.month, delta_days, cur,
            wrap=12, exact=True)

    def _checkWeekday(self, now, delta, cur):
        if isinstance(self.weekday, str):
            self.weekday = int(self.weekday)

        today = now.isoweekday()

        if isinstance(self.weekday, tuple) or isinstance(self.weekday, list):
            if (today in self.weekday):
                if (delta.days >= 1) or self.ignore_delta or cur:
                    return True

        elif self.weekday == today:
            if ((delta.days > 7) or self.ignore_delta or cur):
                return True

        return False

    def checkCron(self, last, now):
        assert(isinstance(now, datetime.datetime))

        if not last:
            last = 0 

        last_date = datetime.datetime.fromtimestamp(float(last))
        delta = now - last_date

        every = False
        run = True
        r = False

        matches = []
        for a, b in self.chk:
            if a:
                r = b(now, delta, r)
                matches.append(r)
                run = run and r

        return run
            

def cron(secs=None, min=None, hour=None, day=None, month=None, weekday=None):
    def wrapper(fn):
        fn.cronable = Cronable(
            fn.__name__, secs, min, hour, day, month, weekday)
        return fn
    return wrapper
