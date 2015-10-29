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

    def checkCron(self, last, now):
        
        if self.weekday:
            
        

def cron(secs=None, min=None, hour=None, day=None, month=None, weekday=None):
    def wrapper(fn):
        fn.cronable = Cronable(
            fn.__name__, secs, min, hour, day, month, weekday)
        return fn
    return wrapper
