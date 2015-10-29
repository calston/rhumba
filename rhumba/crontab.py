import datetime
import calendar

Sunday = 0
Monday = 1
Tuesday = 2
Wednesday = 3
Thursday = 4
Friday = 5
Saturday = 6

class Cronable(object):
    def __init__(self, name, secs, min, hour, day, month, weekday):
        self.name = name
        self.secs = secs
        self.min = min
        self.hour = hour
        self.day = day
        self.month = month
        self.weekday = weekday

    def _checkEvery(self, t, n, total, wrap=59, exact=True):
        if not t:
            return False
        tseg = t

        if isinstance(t, str):
            if t[:2] == '*/':
                tseg = int(t[2:])
                if total >= tseg:
                    return True
                return False
            else:
                tseg = int(t)
       
        if exact:
            if (tseg == n) and (total >= wrap):
                return True
        else:
            if (n >= tseg) and (total >= wrap):
                return True

        return False

    def _checkSecs(self, now, delta):
        total_secs = delta.total_seconds()

        return self._checkEvery(self.secs, now.second, total_secs, exact=False)

    def _checkMins(self, now, delta):
        total_mins = delta.total_seconds() / 60

        return self._checkEvery(self.min, now.minute, total_mins, exact=False)

    def _checkHours(self, now, delta):
        total_hours = delta.total_seconds() / (60*60)

        return self._checkEvery(self.hour, now.hour, total_hours, wrap=23)
    
    def _checkDays(self, now, delta):
        total_days = delta.total_seconds() / (60*60*24)

        # Get the max days in this month
        start, end = calendar.monthrange(now.year, now.month)

        return self._checkEvery(self.day, now.day, total_days, wrap=end-1)

    def checkCron(self, last, now):
        assert(isinstance(now, datetime.datetime))

        if not last:
            last = 0 

        last_date = datetime.datetime.fromtimestamp(float(last))
        delta = now - last_date

        run = False
        

        chk = [
            (self.secs, self._checkSecs(now, delta)),
            (self.min, self._checkMins(now, delta)),
            (self.hour, self._checkHours(now, delta)),
            (self.day, self._checkDays(now, delta))
        ]

        for a, b in chk:
            if a:
                run = b

        if self.weekday == now.isoweekday():
            if delta.days > 7:
                run = True

        return run

            

def cron(secs=None, min=None, hour=None, day=None, month=None, weekday=None):
    def wrapper(fn):
        fn.cronable = Cronable(
            fn.__name__, secs, min, hour, day, month, weekday)
        return fn
    return wrapper
