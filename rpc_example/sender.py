# Imports.
import os
import json
import pika
import uuid

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

class Client(object):

    def __init__(self):

        # Setup RabbitMQ connection.
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        # Queue for responses.
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Exchange for sending.
        self.channel.exchange_declare(exchange='direct_main', exchange_type='direct')

        # Listen for responses.
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    # Handle responses.
    def on_response(self, ch, method, props, body):
        # If response to call.
        if self.corr_id == props.correlation_id:
            # Append response.
            self.response.append(json.loads(body))

    # Make a request.
    def call(self, x):
        self.response = []  # For saving responses to request.
        self.corr_id = str(uuid.uuid4())  # For tracking responses to request.
        self.channel.basic_publish(
            exchange='direct_main',
            routing_key='all_nodes',  # Send to all nodes.
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(x))  # This assumes that x is a dict.
        while len(self.response) < 2:  # Minimum responses needed. Set to 2 for testing.
            self.connection.process_data_events()
        return self.response

# Make a request.
rpc = Client()
print(" [x] Requesting...")
data = {'sender': nodename}
print(" [.] Sent: ", data)
response = rpc.call(data)
print(" [.] Got: ", response)
