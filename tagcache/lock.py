# -*- encoding: utf-8 -*-

import os
import fcntl


class FileLock(object):

    def __init__(self, fd):

        # the fd is borrowed, so do not close it
        self.fd = fd

    def acquire(self, ex=False, nb=True):
        """
        Acquire a lock on the fd.

        :param ex (optional): default False, acquire a exclusive lock if True
        :param nb (optional): default True, non blocking if True
        :return: True if acquired

        """

        try:

            lock_flags = fcntl.LOCK_EX if ex else fcntl.LOCK_SH

            if nb:

                lock_flags |= fcntl.LOCK_NB

            fcntl.flock(self.fd, lock_flags)

            return True

        except IOError:
            
            return False

    def release(self):
        """
        Release lock on the fd.

        """

        fcntl.flock(self.fd, fcntl.LOCK_UN)



