# -*- encoding: utf-8 -*-

import errno
import io
import os
import re
import tempfile
from binascii import hexlify
from functools import wraps
from hashlib import md5
from time import time

from tagcache.lock import FileLock
from tagcache.serialize import PickleSerializer
from tagcache.utils import cached_property, link_file, rename_file, \
        silent_close, silent_unlink, ensure_dir


# datetime.fromtimestamp(2**32)  -> datetime.datetime(2106, 2, 7, 14, 28, 16)
_future_timestamp = 2**32


class Cache(object):
    """

    'key' and 'tags' can only contains [a-zA-Z0-9\-_\.@].

    """

    key_matcher = re.compile(r'^[A-z0-9\-_\.@]+$').match

    def __init__(self, hash_method=md5, serializer=None):
        """
        Create a cache object. `hash_method` is used to hash keys
        into file path; `serializer` is used to dump/load object
        to/from io
        
        """

        self.hash_method = hash_method

        if serializer is None:

            serializer = PickleSerializer()

        self.serializer = serializer

    def configure(self, main_dir):
        """
        Configure the cache.

        :param main_dir: the dir contains everything.

        """

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

    def key_to_path(self, name):

        return self.name_to_path(name, ns='key')

    def tag_to_path(self, name):

        return self.name_to_path(name, ns='tag')

    def invalidate_tag(self, tag):
        """
        Invalidate cache with the tag.

        """
        if not self.key_matcher(tag):

            raise ValueError("Bad tag format {0!r}".format(tag))

        silent_unlink(self.tag_to_path(tag))

    def invalidate_key(self, key):
        """
        Invalidate cache with the key.

        """

        if not self.key_matcher(key):

            raise ValueError("Bad key format {0!r}".format(key))

        silent_unlink(self.key_to_path(key))

    def __call__(self, key, expire=None, tags=None):
        """
        Main decorator. Example usage:

            cache = Cache()
            cache.configure('/var/cache')

            @cache('blog-home', expire=3600*24*7, tags=('home', 'blog'))
            def blog_home():
                ...
                ...

            value = blog_home()

        """

        if not self.key_matcher(key):

            raise ValueError("Bad key format {0!r}".format(key))

        if expire is not None and not isinstance(expire, int):

            raise TypeError(
                "Expect integer value for expire but got {0!r}".format(
                type(expire)))

        if tags:

            for tag in tags:

                if not self.key_matcher(tag):

                    raise ValueError("Bad tag format {0!r}".format(tag))

        def ret(content_fn):

            if not callable(content_fn):

                raise ValueError("Expect callable content function")

            return CacheItem(self, key, content_fn, expire=expire,
                    tags=tags)

        return ret


class CacheItem(object):

    def __init__(self, cache, key, content_fn, expire=None, tags=None):
        """
        CacheItem represents a single cached item with key 'key' and
        optional some 'tags'.

        The format in file is:

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

        :param cache: Cache object.
        :param key: the key for this cache item.
        :param content_fn: the function to generate content on demand, the
            function should return a seekable io.BufferedIOBase object.
        :param expire (optional): default None (never expire), expire interval
            in seconds (int).
        :param tags (optional): default None (no tag), list of tag names.

        """
        self.cache = cache

        self.key = key

        self.content_fn = content_fn

        self.expire = expire

        self.tags = set(tags or [])

    @cached_property
    def path(self):

        return os.path.abspath(self.cache.key_to_path(self.key))

    @cached_property
    def lock_path(self):

        return self.path + '.lock'

    def __call__(self):
        """
        Get content of this cache item. `content_fn` may be called when
        cache miss.

        :return: io.BufferedIOBase object

        """

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
            key, content_io, expire, tags = self._load(f)

            # If the cache is invalid, unless we can get the exclusive
            # lock immediately (non-blocking), use the old one.
            if not self._check(f, key, expire, tags) and \
                    lock.acquire(ex=True, nb=True):

                return self._generate()

            return self.cache.serializer.deserialize(content_io)

        finally:

            lock.release()

            if f is not None:

                f.close()

    def _generate(self):

        content = self.content_fn()

        # Do not cache the generated object.
        if isinstance(content, NotCache):

            return content.not_cache_object

        # Serialize content into io.
        content_io = self.cache.serializer.serialize(content)

        tmp_file = None

        try:

            # Create temp file.
            tmp_file = tempfile.NamedTemporaryFile(
                    dir=self.cache.tmp_dir, delete=False)

            expire_time = _future_timestamp if not self.expire else \
                    int(time()) + self.expire

            # Write meta.
            tmp_file.write('\n'.join([
                self.key,
                bytes(expire_time),
                ":".join(self.tags),
            ]) + '\n')

            # Write content.
            tmp_file.write(content_io.read())

            if self.tags:

                # Using the inode as tag file name.
                st = os.fstat(tmp_file.file.fileno())

                tag_file_name = '{0}'.format(st.st_ino)

                sub_dir = tag_file_name[-2:]

                # Hard link tags.
                for tag in self.tags:

                    tag_file_path = os.path.join(
                            self.cache.tag_to_path(tag), sub_dir,
                            tag_file_name)

                    link_file(tmp_file.name, tag_file_path)

            # XXX: Change mtime of the cache file to the expire time.
            os.utime(tmp_file.name, (expire_time, expire_time))

            # Final step. Move the tmp file to destination. This is
            # an atomic op.
            rename_file(tmp_file.name, self.path)

            return content

        finally:

            if tmp_file is not None:

                try:

                    tmp_file.close()

                except:
                    
                    pass

                silent_unlink(tmp_file.name)

    def _check(self, f, key, expire, tags):

        if key != self.key:

            raise RuntimeError("Load {0!r} but got content of {1!r}".format(
                self.key, key))

        # Check expiration.
        if expire < time():

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

        return key, f, expire, tags


class NotCache(object):
    """
    Sometimes you don't want to cache the object you return.

    """

    def __init__(self, not_cache_object):

        self.not_cache_object = not_cache_object


