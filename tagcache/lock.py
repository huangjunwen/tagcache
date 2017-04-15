# -*- encoding: utf-8 -*-

import os
import fcntl


class FileLock(object):

    def __init__(self, path):

        self.path = path

        self.fd = None

    def acquire(self, write=False, block=True):

        if self.fd is not None:

            self.release()

        try:
            # open or create the file
            open_flags = os.O_RDWR if write else os.O_RDONLY

            open_flags |= os.O_CREAT

            self.fd = os.open(self.path, open_flags)

            # try to lock the file
            lock_flags = fcntl.LOCK_EX if write else fcntl.LOCK_SH

            if not block:

                lock_flags |= fcntl.LOCK_NB

            fcntl.flock(self.fd, lock_flags)

            return self.fd

        #except (OSError, IOError):
        except:

            # open file failed or lock failed
            if self.fd is not None:

                os.close(self.fd)

                self.fd = None

        return None

    def release(self):

        if self.fd is None:

            return

        os.close(self.fd)

        self.fd = None



