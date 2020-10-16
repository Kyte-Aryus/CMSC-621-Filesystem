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

    def _real_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.join(self.root, path)

    # Functions which are not implemented
    getxattr = None
    listxattr = None
    mkdir = None
    rmdir = None
    chmod = None
    chown = None
    symlink = None
    readlink = None
    link = None

    # Functions which can be base functions with adjusted paths

    def mknod(self, path, mode, dev):
        return os.mknod(self._real_path(path), mode, dev)

    def utimens(self, path, times=None):
        return os.utime(self._real_path(path), times)

    def getattr(self, path, fh=None):
        st = os.lstat(self._real_path(path))
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid',
            'st_dev', "st_ino", "st_rdev", "st_blksize", "st_blocks"))

    def statfs(self, path):
        stv = os.statvfs(self._real_path(path))
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(self._real_path(path))

    def access(self, path, mode):
        if not os.access(self._real_path(path), mode):
            raise FuseOSError(EACCES)

    # Functions with custom logic

    def open(self, path, flags):
        return os.open(self._real_path(path), flags)

    def create(self, path, mode):
        return os.open(self._real_path(path), os.O_WRONLY | os.O_CREAT, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        if datasync != 0:
          return os.fdatasync(fh)
        else:
          return os.fsync(fh)

    def read(self, path, size, offset, fh):
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(self._real_path(old), self._real_path(new))

    def truncate(self, path, length, fh=None):
        with open(self._real_path(path), 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        return os.unlink(self._real_path(path))

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
