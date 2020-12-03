import os
import sys
import pika
import json
import base64
import uuid

# Get queue name.
queue = "rabbit@node" + str(sys.argv[1])
routing_key = "direct." + queue + ".propose_update"

# Get file name, offset, and data to write.
path = str(sys.argv[2])
offset = int(sys.argv[3])
data = str(sys.argv[4]).encode('utf-8')
data_length = len(data)

# Get the local version ID.
local_version_id = int(sys.argv[5])

# Connect.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct', exchange_type='topic')

# Encode the data bytes for serialization.
data = base64.b64encode(data) 
data = data.decode('ascii')

# Message data.
msg = {'sender': os.environ['RABBITMQ_NODENAME'], 'file': path, 'data': data, 'offset': offset, 'version_id': local_version_id}

# Send a message to the node. 
channel.basic_publish(exchange='direct', routing_key=routing_key, body=json.dumps(msg),
            properties=pika.BasicProperties(
                reply_to="response_queue_" + os.environ['RABBITMQ_NODENAME'],
                correlation_id=str(uuid.uuid4()),
                expiration='500'
            )
)

# Close connection.
connection.close()
