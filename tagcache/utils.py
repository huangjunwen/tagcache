# -*- encoding: utf-8 -*-

import os
import errno


def ensure_intermediate_dir(path):
    """
    Basiclly equivalent to command `mkdir -p`

    """

    try:

        os.makedirs(os.path.dirname(path))

    except OSError, e:

        if e.errno != errno.EEXIST:

            raise e


def open_file(filename, flag, mode=0777):
    """
    Wrapper of `os.open` which ensure intermediate dirs are created as well.

    """
    try:

        return os.open(filename, flag, mode)

    except OSError, e:

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

    except OSError, e:

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

    except OSError, e:

        if e.errno != errno.ENOENT:

            raise e

        ensure_intermediate_dir(new)

        return os.rename(old, new)
