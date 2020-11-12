import os
import json
import pika

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

# Connect.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct_main', exchange_type='direct')

# Message data.
data = {'sender': nodename, 'event_type': 'something'}

# Send a message to node0 only. 
channel.basic_publish(exchange='direct_main', routing_key='rabbit@node0', body=json.dumps(data))
print('Sent!')

# Close connection.
connection.close()
