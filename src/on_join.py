# Check that the node has joined before running this. 
# Check can be done with monitor or with `rabbitmqctl cluster_status`.

import os
import json
import pika

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

# Connect.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct_main', exchange_type='direct')

# Message data.
data = {'sender': nodename, 'event_type': 'cluster_join'}

# Send a message to all nodes. 
channel.basic_publish(exchange='direct_main', routing_key='all_nodes', body=json.dumps(data))
print('Sent!')

# Close connection.
connection.close()
