import os
import sys
import pika
import json

# Get queue name
queue = "rabbit@node" + str(sys.argv[1])
routing_key = "direct." + queue + ".shutdown"

print(routing_key)

# Connect.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='direct', exchange_type='topic')

# Message data.
data = {'sender': os.environ['RABBITMQ_NODENAME']}

# Send a message to all nodes. 
channel.basic_publish(exchange='direct', routing_key=routing_key, body=json.dumps(data))

# Close connection.
connection.close()
