from .simple_returns import *

def no_op():
    pass

def timed_sleep(seconds: int, additional: int = 0):
    import time

    time.sleep(seconds+additional)