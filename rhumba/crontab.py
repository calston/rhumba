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

    def _checkEvery(self, t, n, delta, wrap=59, exact=True, repeat=False):
        if not t:
            return False, False
        tseg = t

        if isinstance(t, str):
            if t[:2] == '*/':
                tseg = int(t[2:])
                if delta >= tseg:
                    return True, True
                return True, False
            elif '-' in t:
                ra_start, ra_stop = [int(i) for i in t.split('-')]
                tseg = range(ra_start, ra_stop+1)
            else:
                tseg = int(t)

        if (isinstance(tseg, list) or isinstance(tseg, tuple)):
            if (delta>=1) and (n in tseg):
                return True, True

        if exact:
            if (tseg == n) and ((delta >= wrap) or repeat):
                return False, True
        else:
            if (n >= tseg) and ((delta >= wrap) or repeat):
                return False, True

        return False, False

    def _checkSecs(self, now, delta, repeat=False):
        delta_secs = delta.total_seconds()

        return self._checkEvery(
            self.secs, now.second, delta_secs, exact=False, repeat=repeat)

    def _checkMins(self, now, delta, repeat=False):
        delta_mins = delta.total_seconds() / 60

        return self._checkEvery(
            self.min, now.minute, delta_mins, exact=False, repeat=repeat)

    def _checkHours(self, now, delta, repeat=False):
        delta_hours = delta.total_seconds() / (60*60)

        return self._checkEvery(
            self.hour, now.hour, delta_hours, wrap=23, repeat=repeat)
    
    def _checkDays(self, now, delta, repeat=False):
        delta_days = delta.total_seconds() / (60*60*24)

        # Get the max days in this month
        start, end = calendar.monthrange(now.year, now.month)

        return self._checkEvery(
            self.day, now.day, delta_days, wrap=end, repeat=repeat)

    def _checkMonth(self, now, delta, repeat=False):
        # Get the max days in this month
        if now.month == 1:
            lastm = 12
            year = now.year - 1
        else:
            lastm = now.month - 1
            year = now.year

        start, end = calendar.monthrange(year, lastm)

        delta_days = delta.total_seconds() / (60*60*24*end)

        return self._checkEvery(self.month, now.month, delta_days, wrap=12,
            exact=True, repeat=repeat)

    def _checkWeekday(self, now, delta, repeat=False):
        if isinstance(self.weekday, str):
            self.weekday = int(self.weekday)

        today = now.isoweekday()

        if isinstance(self.weekday, tuple) or isinstance(self.weekday, list):
            if (today in self.weekday) and ((delta.days >= 1) or repeat):
                return False, True

        else:
            if (self.weekday == today) and ((delta.days > 7) or repeat):
                return False, True

        return False, False

    def checkCron(self, last, now):
        assert(isinstance(now, datetime.datetime))

        if not last:
            last = 0 

        last_date = datetime.datetime.fromtimestamp(float(last))
        delta = now - last_date

        run = False

        chk = [
            (self.secs, self._checkSecs),
            (self.min, self._checkMins),
            (self.hour, self._checkHours),
            (self.weekday, self._checkWeekday),
            (self.day, self._checkDays),
            (self.month, self._checkMonth),
        ]

        every = False

        for a, b in chk:
            if a:
                e, run = b(now, delta, repeat=every)
                every = (run and e) or every

        return run
            

def cron(secs=None, min=None, hour=None, day=None, month=None, weekday=None):
    def wrapper(fn):
        fn.cronable = Cronable(
            fn.__name__, secs, min, hour, day, month, weekday)
        return fn
    return wrapper
