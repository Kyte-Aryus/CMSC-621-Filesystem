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
import itertools
import copy
import shutil
import operator

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class CABNfs(LoggingMixIn, Operations):

    def __init__(self, root, replication_factor):

        # Structural setup
        self.replication_factor = int(replication_factor)
        self.root = os.path.realpath(root)
        self.rwlock = threading.Lock()
        self.messaging_lock = threading.Lock()
        self.max_node_count = int(os.environ['NODE_COUNT'])

        # Rabbitmq setup
        self.node_name = os.environ['RABBITMQ_NODENAME']
        self.responses = list()
        self.expecting_responses = False
        self.curr_response_corr_id = None
        self.requests_timeout = 1.5  # One second more than TTL
        self.requests_ttl = '500'

        # Needed queues
        self.broadcast_queuename = "broadcast_queue_" + self.node_name
        self.broadcast_queue = None
        self.response_queuename = "response_queue_" + self.node_name
        self.response_queue = None
        self.direct_queuename = "direct_queue_" + self.node_name
        self.direct_queue = None

        # Setup main thread connection
        self.rabbitmq_main_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rabbitmq_main_channel = self.rabbitmq_main_connection.channel()

        # Start threads
        broadcast_listening_thread = threading.Thread(target=self.broadcast_listening_thread_function, daemon=True)
        broadcast_listening_thread.start()
        response_listening_thread = threading.Thread(target=self.response_listening_thread_function, daemon=True)
        response_listening_thread.start()
        direct_listening_thread = threading.Thread(target=self.direct_listening_thread_function, daemon=True)
        direct_listening_thread.start()

        # Define file and status tracking
        self.current_node_alive_count = 1
        self.file_replica_map = dict()  # Only relevent if we're primary
        self.file_primary_map = dict()
        self.local_version_id_dict = dict()
        self.given_leases_dict = dict()  # Lease can only if we're the primary for the file
        self.held_leases_dict = dict()  # Strictly for writing

        # Open the versionID file and load
        self.version_file_path = os.environ['VERSION_ID_FILE']
        self.local_version_id_dict = self.read_version_file()

        # Scan local files on the system and put them in a list
        local_files = list()
        for file in os.listdir(root):
            local_files.append(os.path.basename(file))

        for file in local_files:

            # If local file doesn't have a version ID, make it one
            # Not that this indicates an inconsistency and should never happen
            if file not in self.local_version_id_dict:
                logging.warning("Version ID not found for file: " + file)
                self.local_version_id_dict[file] = 1

        print("Local files")
        print(self.local_version_id_dict)

        # Request all files in the system
        responses = self.process_request_with_response({}, 'broadcast', 'broadcast.request.files',
                                                       self.max_node_count - 1)
        # Iterate through each file in each response
        for response in responses:
            files = response['files']
            for file in files:

                primary = files[file]

                print("File: " + file + " - Primary: " + primary)

                # If it's the first time we've seen this file or we don't know it's primary
                if file not in self.file_primary_map or self.file_primary_map[file] is None:
                    self.file_primary_map[file] = primary
                elif file not in self.file_primary_map:
                    self.file_primary_map[file] = None

        # Now add local files to make sure we have them all
        for file in self.local_version_id_dict:
            if file not in self.file_primary_map:
                self.file_primary_map[file] = None

        print("All files")
        print(self.file_primary_map)

        # self.file_primary_map now contains all files,
        # for each file we own that doesn't have a primary, promote this node
        for file in self.file_primary_map:
            if self.file_primary_map[file] is None:
                self.promote_to_primary(file)

        print(self.file_primary_map)

        # At this point, all primaries for all files should be known
        for file in self.file_primary_map:
            if self.file_primary_map[file] is None:
                print("WARNING: Unknown primary for file " + file + " at startup")

    # =================== BEGIN UTILITY FUNCTIONS ===================

    # Constructs queuename for direct messages
    @staticmethod
    def get_direct_topic_prefix(node):
        return "direct." + node + "."

    # Reads version file from disk to find version IDs
    def read_version_file(self):
        version_id_dict = dict()
        if os.path.isfile(self.version_file_path):
            file = open(self.version_file_path, "r")
            for line in file:
                tokens = line.split(':')
                version_id_dict[os.path.basename(tokens[0])] = tokens[1].strip()
            file.close()
        return version_id_dict

    # Updates version file on disk
    def update_version_file(self):
        handle = open(self.version_file_path, "w")
        for file in self.local_version_id_dict:
            handle.write("{0}:{1}\n".format(file, self.local_version_id_dict[file]))
        handle.close()

    # Call this on a relative path or a mount path
    # to get the real path
    def _real_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.join(self.root, path)

    # =================== END UTILITY FUNCTIONS ===================

    # =================== BEGIN LIFECYCLE FUNCTIONS ===================

    # Can only be called if this node is primary for the file
    # It attempts to distribute self.replication_factor - 1 number
    # of replicas across nodes (prioritizing nodes with lowest disk space)
    # If that many replicas cannot be made, it makes as many as possible
    # NOTE: This operation includes deleting replicas off of the nodes it
    # deems shouldn't have a primary
    def balance_replicas(self, file):

        # Verify we're primary
        if file not in self.file_primary_map or self.file_primary_map[file] != self.node_name:
            return

        print("Balancing " + file)

        # Request replica existence and disk space from other nodes
        data = {'file': file}
        responses = self.process_request_with_response(data, 'broadcast', 'broadcast.request.replica_info',
                                                       self.max_node_count - 1)

        # Find disk usages and who has replicas
        node_to_disk_free_map = dict()
        node_has_replica_map = dict()
        current_replica_nodes = list()
        for response in responses:
            node_to_disk_free_map[response['sender']] = response['disk_free']
            node_has_replica_map[response['sender']] = response['has_replica']

            if response['has_replica']:
                current_replica_nodes.append(response['sender'])

        # Sort by disk free and trim to the replication factor - 1
        node_to_disk_free_map = {k: v for k, v in sorted(node_to_disk_free_map.items(), key=lambda item: item[1],
                                                         reverse=True)}
        desired_replication_nodes = dict(itertools.islice(node_to_disk_free_map.items(), self.replication_factor - 1))

        print("Desired replication nodes for " + file + " - " + str(desired_replication_nodes))

        # Now loop through each node that responded and determine what to do
        for node in node_has_replica_map:

            # Case 1, node has replica and shouldn't
            if node_has_replica_map[node] and node not in desired_replication_nodes:

                # Tell it to delete the file
                data = {'file': file}
                response = self.process_request_with_response(data, 'direct',
                                                              self.get_direct_topic_prefix(node) + 'delete_replica', 1)

                # If succeeded, remove as replica holder
                if len(response) != 0:
                    current_replica_nodes.remove(node)

            # Case 2, node doesn't have replica and should
            elif not node_has_replica_map[node] and node in desired_replication_nodes:

                # Send the replica
                if self.replicate_file_to_node(file, node):

                    # If succeeded, add as replica holder
                    current_replica_nodes.append(node)

            # All other cases do nothing

        # Finally, make sure the replica map is up to date
        self.file_replica_map[file] = current_replica_nodes

        # Note that we don't care how many replicas are actually created here, this is a 'best-effort' algorithm
        # without strong guarantees. Due to the nature of the algorithm, all replicas must be consistent
        # and replicas only exist for fault tolerance in the case of disk failure. If an improper number
        # of replicas exist, it will attempt to rebalanced periodically until the proper number exists

    # Determines the primary for a file
    def determine_primary(self, file):

        # If we know the primary, we're done
        if file in self.file_primary_map:
            return self.file_primary_map[file]

        # Need to request from other nodes
        data = {'file': file}
        responses = self.process_request_with_response(data, 'broadcast',
                                                       'broadcast.request.primary_status', self.max_node_count - 1)

        # If we have too many primaries, we need to force them in order until one takes
        # Re-promote it to force this
        if len(responses) > 1:
            for response in responses:
                node = response['sender']
                data = {}
                inner_response = self.process_request_with_response(data, 'direct',
                                                                    self.get_direct_topic_prefix(node) + 'promote', 1)
                if len(inner_response) == 1:
                    return node

            # If none succeeded, nobody is primary
            return None

        # If there is a primary, return it
        elif len(responses) == 1:
            return responses[0]['sender']

        # Otherwise there is none
        return None

    # Promotes current node to primary for a file
    # This is a destructive operation, if any other server
    # is currently the primary, it will be swapped
    def promote_to_primary(self, file):

        # Only allow promotion if we have the file locally
        if file not in self.local_version_id_dict:
            return

        print("Promote to Primary for " + file)

        # Notify all nodes of new primary
        data = {'file': file}
        self.process_request(data, 'broadcast', 'broadcast.update.primary')

        # Update own map
        self.file_primary_map[file] = self.node_name

        # Re-balance replicas while we're here
        self.balance_replicas(file)

    def replicate_file_to_node(self, file, node):

        # Only primary can request a replicate
        if self.file_primary_map[file] != self.node_name:
            print("NOT PRIMARY")
            return

        # Can only replicate if we have a local copy
        if file not in self.local_version_id_dict:
            print("NOT LOCAL")
            return

        print("Attempting to replicate " + file + " on " + node)

        # Fetch file
        if not file.startswith(self.root):
            file = self._real_path(file)

        handle = open(file, 'r')
        contents = handle.read()

        # Send over rabbit MQ to node
        data = {'file': file, 'contents': contents, 'version_id': self.local_version_id_dict[os.path.basename(file)]}
        response = self.process_request_with_response(data, 'direct', self.get_direct_topic_prefix(node) + 'replicate',
                                                      1)

        # Check if succeeded
        if len(response) == 1:
            return True
        return False

    def on_shutdown(self):
        print('Received shutdown command')

    # =================== END LIFECYCLE FUNCTIONS ===================

    # =================== BEGIN FILE OPERATION HELPER FUNCTIONS ===================

    # TODO
    def process_full_delete(self, file):
        return True

        # Check primary status

        # Send replica delete requests

        # Delete locally

        # Update queues

    def delete_local_file(self, file):
        del_path = file
        if not file.startswith(self.root):
            del_path = self._real_path(del_path)

        if os.path.exists(del_path):
            os.unlink(del_path)

            # Clean up queue
            if file in self.local_version_id_dict:
                self.local_version_id_dict.pop(file)

    # =================== END FILE OPERATION HELPER FUNCTIONS ===================

    # =================== BEGIN MESSAGING FUNCTIONS ===================

    # Sends the data on the broadcast exchange given the routing key of topic
    # Returns after timeout or when max_responses are recieved
    # Note that this method returns the responses to preserve thread safety
    def process_request_with_response(self, data, exchange, topic, max_responses):

        # Ensure only one request is processed at a time
        with self.messaging_lock:

            # Prep response node
            self.responses = []
            self.curr_response_corr_id = str(uuid.uuid4())  # For tracking responses to request.

            self.process_request(data, exchange, topic)

            # Nodes with copies of the file will respond that they have copies
            # So the primary can keep a copy of the replica map
            start_time = int(time.time())
            timeout_time = start_time + self.requests_timeout
            while len(self.responses) < max_responses:  # Up to replication factor can respond
                self.rabbitmq_main_connection.process_data_events()
                if time.time() >= timeout_time:
                    break

            return_dict = copy.deepcopy(self.responses)

            print("Responses for: " + topic)
            print(return_dict)

            return return_dict

    # Sends the data on the broadcast exchange given the routing key of topic
    # Does not wait for responses
    def process_request(self, data, exchange, topic):

        # Messages must always have a sender, this just enforces that
        data['sender'] = self.node_name

        print("Sending " + topic + " to " + exchange)

        self.rabbitmq_main_channel.basic_publish(
            exchange=exchange,
            routing_key=topic,
            properties=pika.BasicProperties(
                reply_to=self.response_queuename,
                correlation_id=self.curr_response_corr_id,
                expiration=self.requests_ttl
            ),
            body=json.dumps(data))  # This assumes that x is a dict.

    # Sends reply
    def process_reply(self, data, channel, props):

        # Messages must always have a sender, this just enforces that
        data['sender'] = self.node_name

        channel.basic_publish(exchange='',
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id,
                                                              expiration=self.requests_ttl),
                              body=json.dumps(data))


    # Thread function which handles listening for broadcast messages
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
        def callback(channel, method, props, body):

            # Don't read from this node
            if props.reply_to is not None and props.reply_to == self.response_queuename:
                return

            payload = json.loads(body)
            sender = payload['sender']

            print("Got request for " + method.routing_key)

            if method.routing_key == 'broadcast.request.files':

                files = dict()

                # Send file list with primary status for local files only
                for file in self.local_version_id_dict:
                    if file in self.file_primary_map:
                        files[file] = self.file_primary_map[file]
                    else:
                        files[file] = None

                # Send reply
                data = {'files': files}
                self.process_reply(data, channel, props)

            elif method.routing_key == 'broadcast.request.replica_info':
                file = payload['file']

                # First check if we have replica
                has_replica = (file in self.local_version_id_dict)

                # Now check disk space
                (total, used, free) = shutil.disk_usage(self.root)
                disk_free = float(free)

                # Send message
                data = {'has_replica': has_replica, 'disk_free': disk_free}
                self.process_reply(data, channel, props)

            elif method.routing_key == 'broadcast.request.primary_status':
                file = payload['file']

                # If we're primary, notify
                if file in self.file_primary_map and self.file_primary_map[file] == self.node_name:
                    self.process_reply({}, channel, props)

            elif method.routing_key == 'broadcast.update.primary':
                file = payload['file']

                # Update local primary map
                self.file_primary_map[file] = sender

                # We can stop keeping track of replicas
                self.file_replica_map = dict()

                # Release given leases
                for lease in self.given_leases_dict:
                    print("Remove")

                    #TODO
                    # Send direct.node.revoke_lease message to each lease holder

                self.given_leases_dict = dict()

            else:
                logging.info('Unhandled broadcast message received')

        rabbitmq_broadcast_channel.basic_consume(queue=self.broadcast_queuename,
                                                 on_message_callback=callback, auto_ack=True)
        rabbitmq_broadcast_channel.start_consuming()

    # Thread function which handles listening for responses
    def response_listening_thread_function(self):

        # Rabbitmq reply queue
        rabbitmq_response_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        rabbitmq_response_channel = rabbitmq_response_connection.channel()
        result = rabbitmq_response_channel.queue_declare(queue=self.response_queuename, exclusive=False)
        self.response_queue = result.method.queue

        # Handle responses.
        def callback(ch, method, props, body):
            # If response to call.
            if self.curr_response_corr_id == props.correlation_id:
                # Append response.
                self.responses.append(json.loads(body))

        rabbitmq_response_channel.basic_consume(queue=self.response_queuename,
                                                on_message_callback=callback, auto_ack=True)
        rabbitmq_response_channel.start_consuming()

    # Thread function which handles listening for direct messages
    def direct_listening_thread_function(self):

        # Rabbitmq broadcast queue
        rabbitmq_direct_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        rabbitmq_direct_channel = rabbitmq_direct_connection.channel()
        rabbitmq_direct_channel.exchange_declare(exchange='direct', exchange_type='topic')
        result = rabbitmq_direct_channel.queue_declare(queue=self.direct_queuename, exclusive=False)
        self.direct_queue = result.method.queue

        # Only listen to messages sent to this node with key direct.<node_name>.#
        rabbitmq_direct_channel.queue_bind(exchange='direct', queue=self.direct_queuename,
                                           routing_key='direct.' + self.node_name + '.#')

        # Logic goes here.
        def callback(ch, method, props, body):

            # Don't read from this node
            if props.reply_to is not None and props.reply_to == self.direct_queuename:
                return

            payload = json.loads(body)
            sender = payload['sender']

            print("Got direct message of type " + method.routing_key)

            # Delete requests
            if method.routing_key.endswith('.delete_file'):
                file = payload['file']

                # Only process if primary
                if self.file_primary_map[file] == self.node_name:
                    self.process_full_delete(file)

                    # Send an empty acknowledgement
                    self.process_reply({}, ch, props)

            elif method.routing_key.endswith('.delete_replica'):
                file = payload['file']

                # Verify the request came from the primary
                if self.file_primary_map[file] == sender:

                    # Process delete
                    self.delete_local_file(file)

                    # Send an empty acknowledgement
                    self.process_reply({}, ch, props)

            elif method.routing_key.endswith('.promote'):
                file = payload['file']

                # Verify the request came from the primary
                if self.file_primary_map[file] == sender:
                    self.promote_to_primary(file)

            elif method.routing_key.endswith('.replicate'):
                file = payload['file']
                if not file.startswith(self.root):
                    file = self._real_path(file)

                # Just write replica without checking
                handle = open(file, 'w')
                chars_written = handle.write(payload['contents'])

                # Add to map
                if chars_written > 0:
                    self.local_version_id_dict[os.path.basename(file)] = payload['version_id']

                    # Send an empty acknowledgement
                    self.process_reply({}, ch, props)

            elif method.routing_key.endswith('.revoke_lease'):
                file = payload['file']

                # Revoke the lease
                if file in self.held_leases_dict:
                    self.held_leases_dict.pop(file)

            elif method.routing_key.endswith('.shutdown'):
                self.on_shutdown()

            else:
                print('Unhandled direct message')

        rabbitmq_direct_channel.basic_consume(queue=self.direct_queuename,
                                              on_message_callback=callback, auto_ack=True)
        rabbitmq_direct_channel.start_consuming()

    # =================== END MESSAGING FUNCTIONS ===================

    # =================== BEING FUSE API FUNCTIONS ===================

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

    def readdir(self, path, fh):
        responses = self.process_request_with_response({}, 'broadcast', 'broadcast.request.files',
                                                       self.max_node_count - 1)
        all_files = set()
        for response in responses:
            all_files = all_files.union(response['files'].keys())
        print(all_files)
        return ['.', '..'] + list(all_files)

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

    # =================== END FUSE API FUNCTIONS ===================


if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <root> <mountpoint> <replication_factor>' % argv[0])
        exit(1)

    logging.basicConfig(filename="/app/debug_log.txt", level=logging.INFO)

    # Setup filesystem
    filesystem = CABNfs(argv[1], argv[3])
    fuse = FUSE(filesystem, argv[2], foreground=True)