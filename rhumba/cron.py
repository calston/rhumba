class Cronable(object):
    def __init__(self, time, name):
        self.time = time
        self.name = name

def cron(time):
    def wrapper(fn):
        fn.cronable = Cronable(time, fn.__name__)
        return fn
    return wrapper
