# -*- encoding: utf-8 -*-

import os
import errno


def ensure_dir(path):
    """
    Basiclly equivalent to command `mkdir -p`

    """

    try:

        os.makedirs(path)

    except OSError as e:

        if e.errno != errno.EEXIST:

            raise e

def ensure_intermediate_dir(path):
    """
    Ensure intermediate dir of a path.

    """
    return ensure_dir(os.path.dirname(path))


def open_file(filename, flag, mode=0777):
    """
    Wrapper of `os.open` which ensure intermediate dirs are created as well.

    """
    try:

        return os.open(filename, flag, mode)

    except OSError as e:

        if e.errno != errno.ENOENT or not (flag & os.O_CREAT):

            raise e

        # a directory component not exists
        ensure_intermediate_dir(filename)

        # second try
        return os.open(filename, flag, mode)


def link_file(src, dst):
    """
    Wrapper of `os.link` which ensure intermediate dirs are created as well.

    """
    try:

        return os.link(src, dst)

    except OSError as e:

        if e.errno != errno.ENOENT:

            raise e

        ensure_intermediate_dir(dst)

        return os.link(src, dst)


def rename_file(old, new):
    """
    Wrapper of `os.rename` which ensure intermediate dirs are created as well.

    """
    try:

        return os.rename(old, new)

    except OSError as e:

        if e.errno != errno.ENOENT:

            raise e

        ensure_intermediate_dir(new)

        return os.rename(old, new)


def silent_close(fd):
    """
    Wrapper of `os.close` which do not raise when the fd is bad fd.

    """

    try:

        return os.close(fd)

    except OSError as e:

        if e.errno != errno.EBADF:

            raise e


def silent_unlink(path):
    """
    Wrapper of `os.unlink` which do not raise when the path not exists.

    """

    try:

        return os.unlink(path)

    except OSError as e:

        if e.errno != errno.ENOENT:

            raise e


class cached_property(object):
    """
    Cached property is a kind of descriptor. 
    
    How it works: When accessing 'inst.attr', python will first search
    the attribute in 'inst.__dict__', then its type's '__dict__'.
    
    So if '__get__' is called, there is no such value on the
    instance's '__dict__', then run the original getter and set the value
    into instance's '__dict__'. Next time accessing 'inst.attr' will get
    the value directly.

    """

    def __init__(self, getter):

        self._getter = getter

        self._name = getter.__name__

    def __get__(self, obj, type_):

        if obj is None:

            return self

        val = obj.__dict__[self._name] = self._getter(obj)

        return val
