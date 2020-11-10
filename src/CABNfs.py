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
import uuid
import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class CABNfs(LoggingMixIn, Operations):

    def __init__(self, root, replication_factor):
        self.replication_factor = int(replication_factor)
        self.root = os.path.realpath(root)
        self.rwlock = threading.Lock()
        self.max_node_count = os.environ['NODE_COUNT']

        # Rabbitmq setup
        self.node_name = os.environ['RABBITMQ_NODENAME']
        self.responses = list()
        self.expecting_responses = False
        self.curr_response_corr_id = None
        self.requests_timeout = 0.5  # Half second

        # Needed queues
        self.broadcast_queuename = "broadcast_queue_" + self.node_name
        self.broadcast_queue = None
        self.response_queuename = "response_queue_" + self.node_name
        self.response_queue = None

        # Setup main thread connection
        self.rabbitmq_main_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rabbitmq_main_channel = self.rabbitmq_main_connection.channel()

        # Start threads
        broadcast_listening_thread = threading.Thread(target=self.broadcast_listening_thread_function, daemon=True)
        broadcast_listening_thread.start()
        response_listening_thread = threading.Thread(target=self.response_listening_thread_function, daemon=True)
        response_listening_thread.start()

        # Define file and status tracking
        self.current_node_alive_count = 1
        self.file_primary_map = dict()
        self.local_version_id_dict = dict()
        self.given_leases_dict = dict()  # Lease can only if we're the primary for the file
        self.held_leases_dict = dict()  # Strictly for writing

        # Open the versionID file and load
        self.version_file_path = os.environ['VERSION_ID_FILE']
        self.local_version_id_dict = self.read_version_file()
        print(self.local_version_id_dict)

        # Scan local files on the system and put them in a list
        local_files = list()
        for file in os.listdir(root):
            local_files.append(os.path.join(root, file))

        for file in local_files:

            # If local file doesn't have a version ID, make it one
            # Not that this indicates an inconsistency
            if file not in self.local_version_id_dict:
                logging.warning("Version ID not found for file: " + file)
                self.local_version_id_dict[file] = 1

            # For each local file determine primary server statuses
            self.determine_primary(file)

            # If no primary was found make node the primary
            if file not in self.file_primary_map:

                print('promote')
                # TODO
                #self.promote_to_primary(file)

        # Request all files in system

    # =================== BEGIN UTILITY FUNCTIONS ===================

    # Determines the primary for a file, if none exists and assignPrimary
    # is True, then it will assign this node as primary (with everything
    # that entails) if no primary is found
    def determine_primary(self, filepath):

        # If we know the primary, we're done
        if filepath in self.file_primary_map:
            return self.file_primary_map[filepath]

        # Need to request from other nodes

        # Prep responses
        self.responses = []
        self.curr_response_corr_id = str(uuid.uuid4())  # For tracking responses to request.

        # Request responses
        print('SENDING')
        self.rabbitmq_main_channel.basic_publish(
            exchange='broadcast',
            routing_key='broadcast.request.primary_status',
            properties=pika.BasicProperties(
                reply_to=self.response_queuename,
                correlation_id=self.curr_response_corr_id,
            ),
            body="")  # This assumes that x is a dict.

        start_time = int(time.time())
        timeout_time = start_time + self.requests_timeout
        while len(self.responses) < self.replication_factor:  # Maximum response is number of replications
            self.rabbitmq_main_connection.process_data_events()
            if time.time() >= timeout_time:
                break

        print(self.responses)

        # Check responses
        for response in self.responses:

            # Check if server is primary
            if 'is_primary' in reponse and reponse['is_primary'] == 'True':
                self.file_primary_map[filepath] = response['server']

                # If a primary exists it will either tell this server to keep
                # it's replica or to delete it

                if 'keep_file' in reponse:

                    # Need to update with most recent
                    if response['keep_file'] == 'True':
                        self.update_replica_from_primary(filename)

                    # Else do a remove on the root filesystem
                    elif response['keep_file'] == 'False':
                        del_path = filepath
                        if not filepath.startswith(self.root):
                            del_path = self._real_path(del_path)
                        os.unlink(del_path)

    def read_version_file(self):
        version_id_dict = dict()
        if os.path.isfile(self.version_file_path):
            file = open(self.version_file_path, "r")
            for line in file:
                tokens = line.split(':')
                version_id_dict[tokens[0]] = tokens[1].strip()
            file.close()
        return version_id_dict

    def update_version_file(self):
        file = open(self.version_file_path, "w")
        for filepath in self.local_version_id_dict:
            file.write("{0}:{1}\n".format(filepath, self.local_version_id_dict[filepath]))
        file.close()

    def _real_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.join(self.root, path)

    # =================== END UTILITY FUNCTIONS ===================

    # =================== BEGIN MESSAGING FUNCTIONS ===================

    def broadcast_listening_thread_function(self):

        # Rabbitmq broadcast queue
        rabbitmq_broadcast_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        rabbitmq_broadcast_channel = rabbitmq_broadcast_connection.channel()
        rabbitmq_broadcast_channel.exchange_declare(exchange='broadcast', exchange_type='topic')
        result = rabbitmq_broadcast_channel.queue_declare(queue=self.broadcast_queuename, exclusive=False)
        self.broadcast_queue = result.method.queue
        rabbitmq_broadcast_channel.queue_bind(exchange='broadcast', queue=self.broadcast_queuename,
                                              routing_key='broadcast.#')  # Listen for messages sent to all nodes

        # Logic goes here.
        def callback(ch, method, properties, body):

            print("Recv")

            # Don't read from this node
            if properties.reply_to is not None and properties.reply_to == self.response_queuename:
                print("discard")
                return


            if method.routing_key == 'broadcast.event.join':
                logging.info(' [x] %r joined the cluster with topic')
            elif method.routing_key == 'broadcast.event.leave':
                logging.info(' [x] %r left the cluster with topic')
            elif method.routing_key == 'broadcast.request.primary_status':
                print(' [XXXXX] GOT PRIMARY STATUS REQUEST from %f', properties.reply_to)
            else:
                logging.info(' [x] Unhandled message received.')

        rabbitmq_broadcast_channel.basic_consume(queue=self.broadcast_queuename,
                                                 on_message_callback=callback, auto_ack=True)

        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        rabbitmq_broadcast_channel.start_consuming()

    def response_listening_thread_function(self):

        # Rabbitmq reply queue
        rabbitmq_response_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        rabbitmq_response_channel = rabbitmq_response_connection.channel()
        rabbitmq_response_channel.exchange_declare(exchange='response', exchange_type='direct')
        result = rabbitmq_response_channel.queue_declare(queue=self.response_queuename, exclusive=False)
        self.response_queue = result.method.queue

        # Handle responses.
        def callback(self, ch, method, props, body):
            print("Got response")
            # If response to call.
            if self.curr_response_corr_id == props.correlation_id:
                # Append response.
                self.responses.append(json.loads(body))

        rabbitmq_response_channel.basic_consume(queue=self.response_queuename,
                                                on_message_callback=callback, auto_ack=True)
        rabbitmq_response_channel.start_consuming()

    # =================== END MESSAGING FUNCTIONS ===================

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
                                                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                        'st_uid',
                                                        'st_dev', "st_ino", "st_rdev", "st_blksize", "st_blocks"))

    def statfs(self, path):
        stv = os.statvfs(self._real_path(path))
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
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

    logging.basicConfig(filename="/app/debug_log.txt", level=logging.INFO)

    # Setup filesystem
    filesystem = CABNfs(argv[1], argv[3])
    fuse = FUSE(filesystem, argv[2], foreground=True)
