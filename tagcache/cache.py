# -*- encoding: utf-8 -*-

import errno
import io
import os
import tempfile
from binascii import hexlify
from functools import wraps
from hashlib import md5
from time import time

from tagcache.lock import FileLock
from tagcache.utils import cached_property, link_file, rename_file, \
        silent_close, silent_unlink, ensure_dir


class CacheManager(object):
    """

    'key' and 'tags' can only contains [a-zA-Z0-9\-_\.@].

    """

    def __init__(self, hash_method=md5):

        self.hash_method = hash_method

    def configure(self, main_dir):

        main_dir = os.path.abspath(main_dir)

        if not os.path.isdir(main_dir):

            raise ValueError("{0} is not a directory".format(main_dir))

        self.main_dir = main_dir

    @cached_property
    def data_dir(self):

        ret = os.path.join(self.main_dir, 'data')

        ensure_dir(ret)

        return ret

    @cached_property
    def tmp_dir(self):

        ret = os.path.join(self.main_dir, 'tmp')

        ensure_dir(ret)

        return ret

    def name_to_path(self, name, ns=''):

        prefix = '' if not ns else ns + ':'

        name = prefix + hexlify(name)

        h = self.hash_method(name).hexdigest()

        return os.path.join(self.data_dir, h[:2], h[2:4], name)

    def __call__(self, key, expire=None, tags=None):



        def ret(content_fn):

            return CacheObject(self, key, content_fn, expire=expire,
                    tags=tags)

        return ret



class CacheObject(object):

    def __init__(self, manager, key, content_fn, expire=None, tags=None):
        """
        CacheObject represents a single cached item with key 'key'.

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

        self.key = key

        self.content_fn = content_fn

        self.expire = expire or 0

        self.tags = set(tags or [])

    @cached_property
    def path(self):

        return os.path.abspath(self.manager.name_to_path(self.key, ns='key'))

    @cached_property
    def lock_path(self):

        return self.path + '.lock'

    def __call__(self):

        path = self.path

        lock = FileLock(self.lock_path)

        f = None

        try:

            try:

                f = open(path, 'rb')

            except IOError, e:

                if e.errno != errno.ENOENT:

                    raise e

                # Not exists. Get a exclusive lock (blocking) before generating
                # content.
                if not lock.acquire(ex=True, nb=False):

                    raise RuntimeError(
                            "Can't get exclusive lock on {0!r}".format(path))

                # Since some other may have created the cache during 
                # 'lock.acquire', test it.
                try:

                    f = open(path, 'rb')

                except IOError, e:

                    if e.errno != errno.ENOENT:

                        raise e

                    # Still not exist, create it.
                    return self._generate()

                else:
                    
                    lock.release()

            # There is old cache and no lock here.
            assert f is not None and not lock.is_acquired

            # Load cache.
            key, content, expire, tags = self._load(f)

            # If the cache is invalid, unless we can get the exclusive
            # lock immediately (non-blocking), use the old one.
            if not self._check(f, key, content, expire, tags) and \
                    lock.acquire(ex=True, nb=True):

                return self._generate()

            return content

        finally:

            lock.release()

            if f is not None:

                f.close()

    def _generate(self):

        content = self.content_fn()

        if not isinstance(content, io.BufferedIOBase) or not content.seekable():

            raise TypeError(
                "Expect seekable io.BufferedIOBase instance, but got {0!r}".format(
                type(content)))

        tmp_file = None

        try:

            # Create temp file.
            tmp_file = tempfile.NamedTemporaryFile(
                    dir=self.manager.tmp_dir, delete=False)

            expire_time = 0 if not self.expire else int(time()) + self.expire

            # Write meta.
            tmp_file.write('\n'.join([
                self.key,
                bytes(expire_time),
                ":".join(self.tags),
            ]) + '\n')

            # Write content.
            tmp_file.write(content.read())

            if self.tags:

                # Generate tag file name. XXX: (dev, inode) should be unique?
                st = os.fstat(tmp_file.file.fileno())

                tag_file_name = '{0}.{1}'.format(st.st_dev, st.st_ino)

                # Hard link tags.
                for tag in self.tags:

                    tag_file_path = os.path.join(
                            self.manager.name_to_path(tag, ns='tag'),
                            tag_file_name)

                    link_file(tmp_file.name, tag_file_path)

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

    def _check(self, f, key, content, expire, tags):

        if key != self.key:

            raise RuntimeError("Load {0!r} but got content of {1!r}".format(
                self.key, key))

        # Check expiration.
        if expire and expire < time():

            return False

        # Check tags changed?
        if tags != self.tags:

            return False

        # Check tag validation.
        st = os.fstat(f.fileno())

        if st.st_nlink != len(tags) + 1:

            return False

        return True

    def _load(self, f):

        key = f.readline().strip()

        expire = int(f.readline().strip())

        tags = set(f.readline().strip().split(':'))

        if '' in tags:

            tags.remove('')

        # TODO: make content lazy
        content = io.BytesIO(f.read())

        return key, content, expire, tags
