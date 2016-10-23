#encoding:utf-8

# pylint: disable=superfluous-parens, invalid-name, broad-except

""" my own logging modules """

import threading
import inspect
import os
import datetime

# Logger constant. Can be access by Logger.CONSTANT_NAME
DEBUG = 0
INFO = 1

class Logger:
    """ my little logger """

    # Logger constant. Can be access by Logger.CONSTANT_NAME
    DEBUG = 0
    INFO = 1

    def __init__(self, log_level=DEBUG):
        """ initialization """
        self.log_level = log_level
        stacks = inspect.stack()
        # we want to get the name of the file where 'Logger()' is called
        self.current_file_name = stacks[1][1] if len(stacks) >= 2 else stacks[-1][1]

        log_dir = os.path.join(os.getcwd(), "log")
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        dt = datetime.datetime.now()
        _y = str(getattr(dt, 'year'))
        _m = str(getattr(dt, 'month'))
        _d = str(getattr(dt, 'day'))
        _h = str(getattr(dt, 'hour'))
        _time = _y + '-' + _m + '-' + _d + ',' + _h + 'h'
        log_file_name = (self.current_file_name.split('/')[-1].rstrip(".py")
                         + "--" + _time + ".log")
        log_file_path = os.path.join(log_dir, log_file_name)
        self._log = open(log_file_path, "w")
        self.lock = threading.Lock()

    def acquire_lock(self):
        """ acquire access to self._log """
        self.lock.acquire()

    def release_lock(self):
        """ release lock to self._log """
        self.lock.release()

    def info(self, msg):
        """ logging method. Ignore log_level now """
        self.acquire_lock()
        t = str(datetime.datetime.now())
        self._log.write("[%s] " % t + (msg).strip() + "\n")
        if self.log_level == DEBUG:
            self._log.flush()  # flush buffer
        self.release_lock()

    def write(self, msg):
        """ same as self.info() """
        self.info(msg)
