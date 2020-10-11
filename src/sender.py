import pika

# Connect to local broker.
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Create a queue.
channel.queue_declare(queue='test_queue')

# Send a message. 
channel.basic_publish(exchange='', routing_key='test_queue', body='Hello World!')
print('Sent!')

# Close connection.
connection.close()