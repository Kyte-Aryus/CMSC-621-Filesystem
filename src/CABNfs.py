#!/usr/bin/env python

from __future__ import print_function, absolute_import, division

import logging

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class CABNfs(LoggingMixIn, Operations):

    def __init__(self, root, replication_factor):
        self.replication_factor = int(replication_factor)
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(CABNfs, self).__call__(op, self.root + path, *args)

    # Functions which are not implemented
    getxattr = None
    listxattr = None
    mkdir = None
    rmdir = None
    chmod = None
    chown = None
    symlink = None
    readlink = None

    # Functions which can be base functions
    mknod = os.mknod
    utimens = os.utime

    # Functions with custom logic

    def open(self, path, flags, mode):
        return os.open(path, flags, mode)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    def create(self, path, mode):
        return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def link(self, target, source):
        return os.link(source, target)

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, self.root + new)

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def truncate(self, path, length, fh=None):
        with open(path, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        return os.unlink(path)

    def write(self, path, data, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <root> <mountpoint> <replication_factor>' % argv[0])
        exit(1)

    logging.basicConfig(filename="/app/debug_log.txt", level=logging.DEBUG)

    filesystem = CABNfs(argv[1], argv[3])
    fuse = FUSE(filesystem, argv[2], foreground=True)