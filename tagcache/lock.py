# -*- encoding: utf-8 -*-

import os
import fcntl


class FileLock(object):

    def __init__(self, fd):

        # the fd is borrowed, so do not close it
        self.fd = fd

    def acquire(self, write=False, block=False):

        try:

            lock_flags = fcntl.LOCK_EX if write else fcntl.LOCK_SH

            if not block:

                lock_flags |= fcntl.LOCK_NB

            fcntl.flock(self.fd, lock_flags)

            return True

        except IOError:
            
            return False

    def release(self):

        fcntl.flock(self.fd, fcntl.LOCK_UN)



