#!/usr/bin/env python

from __future__ import print_function, absolute_import, division

from errno import EACCES
from sys import argv, exit
import logging
import threading
import os.path
import json
import pika

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class CABNfs(LoggingMixIn, Operations):

    def __init__(self, root, replication_factor):
        self.replication_factor = int(replication_factor)
        self.root = os.path.realpath(root)
        self.rwlock = threading.Lock()

        # Define maps and lists
        self.currentNodeAliveCount = 1
        self.filePrimaryMap = dict()
        self.localVersionIdMap = dict()
        self.givenLeasesMap = dict()    # Lease can only if we're the primary for the file
        self.heldLeasesMap = dict()     # Strictly for writing

        # Open the versionID file and load
        self.versionFilePath = os.environ['VERSION_ID_FILE']
        self.versionIdMap = dict()
        self.read_version_file()
        print(self.versionIdMap)

        # Scan local files on the system and put them in a list
        self.localFiles = list()
        for file in os.listdir(root):
            self.localFiles.append(os.path.join(root, file))

        # If local file doesn't have a version ID, make it one
        # Not that this indicates an inconsistency
        for file in self.localFiles:
            if file not in self.versionIdMap:
                self.versionIdMap[file] = 1

        # For each local file request primary server statuses
        # NOTE: If a primary exists it will send back its status
        # and either a "replicate" command if this server should
        # hold a replica or a "delete" command if this server should
        # delete its copy

        # If no primary, determine if < replication_factor / 2 responses
        # and select a primary if so

        # Request all files in system





        # Update replica mapping

        # If < replication_factor / 2 responses, no mapping is defined

        # Updating lease mapping

        # Start listening thread
        self.listening_thread = threading.Thread(target=self.listening_thread_client_function, daemon=True)
        self.listening_thread.start()

    def read_version_file(self):
        if os.path.isfile(self.versionFilePath):
            file = open(self.versionFilePath, "r")
            for line in file:
                tokens = line.split(':')
                self.versionIdMap[tokens[0]] = tokens[1].strip()
            file.close()

    def update_version_file(self):
        file = open(self.versionFilePath, "w")
        for filepath in self.versionIdMap:
            file.write("{0}:{1}\n".format(filepath, self.versionIdMap[filepath]))
        file.close()

    def _real_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.join(self.root, path)

    def listening_thread_client_function(self):

        nodename = os.environ['RABBITMQ_NODENAME']

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.exchange_declare(exchange='direct_main', exchange_type='direct')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='direct_main', queue=queue_name,
                           routing_key='all_nodes')  # Listen for messages sent to all nodes.
        channel.queue_bind(exchange='direct_main', queue=queue_name,
                           routing_key=nodename)  # Listen for messages sent to this node.

        # Logic goes here.
        def callback(ch, method, properties, body):
            data = json.loads(body)
            if data['event_type'] == 'cluster_join':
                logging.debug(' [x] %r joined the cluster' % (data['sender']))
            elif data['event_type'] == 'cluster_leave':
                logging.debug(' [x] %r left the cluster' % (data['sender']))
            elif method.routing_key == nodename:
                logging.debug(' [x] Received a personal message!')
            else:
                logging.debug(' [x] Unhandled message received.')

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

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

        # Verify non-existence on other servers

        # Create the file locally

        #

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

    # Setup filesystem
    filesystem = CABNfs(argv[1], argv[3])
    fuse = FUSE(filesystem, argv[2], foreground=True)
