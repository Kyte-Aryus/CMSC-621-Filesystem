# Imports.
import os
import json
import pika

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

# Setup RabbitMQ connection.
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Set up listening.
channel.exchange_declare(exchange='direct_main', exchange_type='direct')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='direct_main', queue=queue_name, routing_key='all_nodes')  # Listen for messages sent to all nodes.

# Do the desired processing.
def compute(x):
    # Just returns the data it receives plus its nodename. 
    data = json.loads(x)
    data['worker'] = nodename
    print(data)
    return json.dumps(data)

# Handle incoming requests.
def on_request(ch, method, props, body):
    response = compute(body)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests.")
channel.start_consuming()
