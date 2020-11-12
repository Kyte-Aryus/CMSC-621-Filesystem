# Check that the node has joined before running this. 
# Check can be done with monitor or with `rabbitmqctl cluster_status`.

import os
import json
import pika

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

# Connect.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='broadcast', exchange_type='topic')

# Message data.
data = {'sender': nodename, 'event_type': 'cluster_join'}

# Send a message to all nodes. 
channel.basic_publish(exchange='broadcast', routing_key='broadcast.event.join', body=json.dumps(data))
print('Sent! %r:%r' % ("broadcast.event.join", json.dumps(data)))

# Close connection.
connection.close()
