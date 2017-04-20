# -*- encoding: utf-8 -*-

import errno
import fcntl
import io
import os
import re
import shutil
import tempfile
from binascii import hexlify
from functools import wraps
from hashlib import md5
from time import time

from tagcache.serialize import PickleSerializer
from tagcache.utils import cached_property, link_file, rename_file, \
        silent_close, silent_unlink, ensure_dir


# datetime.fromtimestamp(2**32)  -> datetime.datetime(2106, 2, 7, 14, 28, 16)
_future_timestamp = 2**32


class Cache(object):
    """
    Example usage:

        cache = Cache()
        cache.configure('/tmp/blog_cache')

        @cache('blog-home', expire=3600*24*7, tags=('blog-new', 'bio'))
        def home_page_content(cache_param):

            ...
            # sometimes content is not available and you don't want
            # to cache the return value.
            cache_param.disable()

            # sometimes tags are only known at runtime
            cache_param.tags.add('some-more-tag')

            # sometimes you want to change expire
            cache_param.expire = 3600*24

            # generate home page content ...
            ...
            return {"bio": {...}, "recent_blogs": [...]}

        content = home_page_content()

    """

    # 'key' and 'tags' can only contains [a-zA-Z0-9\-_\.@].
    key_matcher = re.compile(r'^[A-z0-9\-_\.@]+$').match

    key_ns = 'k'

    tag_ns = 't'

    def __init__(self, main_dir=None, hash_method=md5, serializer=None):
        """
        Create a cache object. `hash_method` is used to hash keys
        into file path; `serializer` is used to dump/load object
        to/from io
        
        """

        self.hash_method = hash_method

        if serializer is None:

            serializer = PickleSerializer()

        self.serializer = serializer

        if main_dir is not None:

            self.configure(main_dir)

    def configure(self, main_dir):
        """
        Configure the cache.

        :param main_dir: the dir contains everything.

        """
        if hasattr(self, 'main_dir'):

            raise RuntimeError("`configure` has been called")

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

        return self.name_to_path(name, ns=self.key_ns)

    def tag_to_path(self, name):

        return self.name_to_path(name, ns=self.tag_ns)

    def invalidate_tag(self, tag):
        """
        Invalidate cache with the tag.

        """
        if not self.key_matcher(tag):

            raise ValueError("Bad tag format {0!r}".format(tag))

        shutil.rmtree(self.tag_to_path(tag), ignore_errors=True)

    def invalidate_key(self, key):
        """
        Invalidate cache with the key.

        """

        if not self.key_matcher(key):

            raise ValueError("Bad key format {0!r}".format(key))

        os.utime(self.key_to_path(key), None)

    def __call__(self, key, expire=None, tags=None):
        """
        Main decorator.

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

    def cleanup(self):
        """
        Clean expired or invalid files.

        :return: (unlink keys count, unlink tag links count)

        """
        now = time()

        def listdir(path):

            for item in os.listdir(path):

                yield os.path.join(path, item)

        def handle_key(path):

            try:

                st = os.stat(path)

            except OSError:

                return False

            # expire < now
            if st.st_mtime < now:

                return silent_unlink(path)

            return False

        def handle_tag_link(path):

            try:

                st = os.stat(path)

            except OSError:

                return False

            links = int(os.path.basename(path).split(':')[0])

            # expire < now or links changes
            if st.st_mtime < now or links != st.st_nlink:

                # make it expire
                try:

                    os.utime(path, None)

                except OSError:

                    pass

                return silent_unlink(path)

            return False

        unlink_keys = 0

        unlink_tag_links = 0

        for lvl1 in listdir(self.data_dir):

            for lvl2 in listdir(lvl1):

                for lvl3 in listdir(lvl2):

                    ns = os.path.basename(lvl3).split(':')[0]

                    # For keys.
                    if ns == self.key_ns:

                        if handle_key(lvl3):

                            unlink_keys += 1

                    # For tag links.
                    elif ns == self.tag_ns:

                        for lvl4 in listdir(lvl3):

                            for lvl5 in listdir(lvl4):

                                if handle_tag_link(lvl5):

                                    unlink_tag_links += 1

        return unlink_keys, unlink_tag_links


class CacheItem(object):

    class _CacheParam(object):
        """
        The param of content function.

        """

        def __init__(self, expire, tags):

            self.expire = expire

            self.tags = tags

            self.disabled = False

        def disable(self):

            self.disabled = True

    def __init__(self, cache, key, content_fn, expire=None, tags=None):
        """
        CacheItem represents a single cache item with key 'key', an
        optinal an expiration, some optional 'tags'. When expiration
        reached or some of its tags is invalidated, the item is considered
        out of date.

        The format in cache file is:

        ```
        tag1:tag2:tag3
        content
        ```

        line 1 contains the tags seperated by ':'.
        line 2 and so on the real payload.

        :param cache: Cache object.
        :param key: the key for this cache item.
        :param content_fn: the function to generate content on demand, the
            function should return serializable object.
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

    def __call__(self):
        """
        Get content of this cache item. `content_fn` may be called when
        cache miss.

        """
        f = None

        try:

            try:

                f = open(self.path, 'rb')

            except IOError as e:

                if e.errno != errno.ENOENT:

                    raise e

                return self._generate()

            expired, tags_invalid, content_io = self._load(f)

            if expired or tags_invalid:

                if self._acquire_flock(f.fileno()):

                    return self._generate()

            # Use old cache.
            return self.cache.serializer.deserialize(content_io)

        finally:

            if f is not None:

                try:
                    
                    f.close()

                except:

                    pass

    def _acquire_flock(self, fd):

        try:

            fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)

            return True

        except IOError:

            return False

    def _generate(self):

        # Copy params.
        param = self._CacheParam(self.expire, self.tags.copy())

        content = self.content_fn(param)

        # Do not cache?
        if param.disabled:

            return content

        # Prepare params.
        content_io = self.cache.serializer.serialize(content)

        tags = param.tags
        
        expire_time = _future_timestamp if not param.expire else \
                int(time()) + param.expire

        tmp_file = tmp_file_path = None

        try:

            # Create temp file.
            tmp_file = tempfile.NamedTemporaryFile(
                    dir=self.cache.tmp_dir, delete=False)

            tmp_file_path = tmp_file.name

            # Write content.
            tmp_file.write('\n'.join([
                ":".join(tags),
                content_io.read(),
            ]))

            # Link tags.
            if tags:

                # Using the nlinks + inode as tag file name.
                st = os.fstat(tmp_file.fileno())

                tag_file_name = '{0}:{1}'.format(len(tags)+1, st.st_ino)

                sub_dir = tag_file_name[-2:]

                # Hard link tags.
                for tag in tags:

                    tag_file_path = os.path.join(
                            self.cache.tag_to_path(tag), sub_dir,
                            tag_file_name)

                    link_file(tmp_file.name, tag_file_path)

            # Close it.
            tmp_file.close()

            # No more use tmp_file.
            tmp_file = None

            # Using mtime as expire time.
            os.utime(tmp_file_path, (expire_time, expire_time))

            # Final step. Move the tmp file to destination. This is
            # an atomic op.
            rename_file(tmp_file_path, self.path)

            # No more use tmp_file_path.
            tmp_file_path = None

            return content

        finally:

            if tmp_file is not None:

                try:

                    tmp_file.close()

                except:
                    
                    pass

            if tmp_file_path is not None:

                silent_unlink(tmp_file_path)

    def _load(self, f):

        # 'mtime' stores the expire time.
        st = os.fstat(f.fileno())

        expired = st.st_mtime < time()

        # Check tags and nlink.
        tags = set(f.readline().strip().split(':'))

        if '' in tags:

            tags.remove('')

        tags_invalid = len(tags) + 1 != st.st_nlink

        return expired, tags_invalid, f
