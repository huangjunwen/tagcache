# -*- encoding: utf-8 -*-

import os
import fcntl

from tagcache.utils import open_file


class FileLock(object):
    """
    From flock(2) on linux:
    
        ...
        If a process uses open(2) (or similar) to obtain more than one 
        descriptor for the same file, these descriptors are treated 
        independently by flock().
        ...

    and on bsd:

        ...
        Locks are on files, not file descriptors.
        ...

    So the file lock open new fd in each instance to provide lock
    sematic in multi-threads/processes enviroment.
        
    """

    def __init__(self, path):

        self._path = path

        self._fd = None

    @property
    def is_acquired(self):

        return self._fd is not None

    def acquire(self, ex=False, nb=False):
        """
        Acquire a lock on a path.

        :param ex (optional): default False, acquire a exclusive lock if True
        :param nb (optional): default False, non blocking if True
        :return: True on success
        :raise: raise RuntimeError if a lock has been acquired

        """
        if self._fd is not None:

            raise RuntimeError("A lock has been held")

        try:

            # open or create the lock file
            self._fd = open_file(self._path, os.O_RDWR|os.O_CREAT)

            lock_flags = fcntl.LOCK_EX if ex else fcntl.LOCK_SH

            if nb:

                lock_flags |= fcntl.LOCK_NB

            fcntl.flock(self._fd, lock_flags)

            return True

        except Exception, e:

            if self._fd is not None:

                os.close(self._fd)
                
                self._fd = None
            
            return False

    def release(self):
        """
        Release the lock.

        """
        if self._fd is None:

            return

        fcntl.flock(self._fd, fcntl.LOCK_UN)

        os.close(self._fd)

        self._fd = None



