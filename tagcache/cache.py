# -*- encoding: utf-8 -*-

import errno
import io
import os
import tempfile

from tagcache.lock import FileLock
from tagcache.utils import cached_property, link_file, rename_file, \
        silent_close, silent_unlink


class FileCacheObject(object):

    def __init__(self, manager, fn, key, expire=None, tags=None):
        """
        FileCacheObject represents a single cached item with key 'key'.

        'key' and 'tags' can only contains [a-zA-Z0-9\-_\.@].

        The format in a file cache is:

        ```
        key
        1492334827
        tag1:tag2:tag3
        content
        ```

        line 1 contains the key.
        line 2 contains the expire time (in string).
        line 3 contains the tags seperated by ':'.
        line 4 and so on the real payload.

        """
        self.manager = manager

        self.fn = fn

        self.key = key

        self.expire = expire or 0

        self.tags = tags or []

    @cached_property
    def path(self):

        return os.path.abspath(self.manager.key2path(self.key))

    @cached_property
    def lock_path(self):

        return self.path + '.lock'

    def __call__(self):

        path = self.path

        lock = FileLock(self.lock_path)

        fd = None

        try:

            try:

                fd = os.open(path, os.O_RDONLY)

            except OSError, e:

                if e.errno != errno.ENOENT:

                    raise e

                # Not exists. Get a exclusive lock (blocking) before generating
                # content.
                if not lock.acquire(ex=True, nb=False):

                    raise RuntimeError("Can't get exclusive lock on %r" % path)

                # Since some other may have created the cache during 
                # 'lock.acquire', test it.
                try:

                    fd = os.open(path, os.O_RDONLY)

                except OSError, e:

                    if e.errno != errno.ENOENT:

                        raise e

                    # Still not exist, create it.
                    return self._generate()

                else:
                    
                    lock.release()

            # There is old cache and no lock here.
            assert fd is not None and not lock.is_acquired

            cache = self._load(fd)
            
            # If the cache is invalid, unless we can get the exclusive
            # lock immediately (non-blocking), use the old one.
            if not cache.is_valid and lock.acquire(ex=True, nb=True):

                return self._generate()

            return cache

        finally:

            lock.release()

            if fd is not None:

                os.silent_close(fd)

    def _generate(self):

        content = self.fn()

        if not isinstance(content, io.IOBase) or not content.seekable():

            raise TypeError("Expect seekable io.IOBase instance, but got %r" %\
                    type(content))

        tmp_file = None

        try:

            # Create temp file.
            tmp_file = tempfile.NamedTemporaryFile(dir=self.manager.tmp_dir,
                    delete=False)

            # Write meta.
            tmp_file.writelines([
                self.key,
                bytes(self.expire),
                ":".join(self.tags),
            ])

            # Write content.
            tmp_file.writelines(content.readlines())

            st = os.fstat(tmp_file.file.fileno())

            # (dev, inode) should be unique?
            tag_file_name = '{0}.{1}'.format(st.st_dev, st.st_ino)

            # Hard link tags.
            for tag in self.tags:

                tag_file_path = os.path.join(self.manager.tag2path(tag),
                        tag_file_name)

                link_file(tmp_file.name, tag_file_path)

            # Now the file should have len(tags) + 1 links.
            st = os.fstat(tmp_file.file.fileno())

            if st.st_nlink != len(self.tags) + 1:

                # Should not happen.
                raise RuntimeError("Links != len(tags) + 1")

            # Final step. Move the tmp file to destination. This is
            # an atomic op.
            rename_file(tmp_file.name, self.path)

            # Seek back content.
            content.seek(0)

            return content

        finally:

            if tmp_file is not None:

                tmp_file.close()

                silent_unlink(tmp_file.name)

    def _load(self, fd):

        pass
