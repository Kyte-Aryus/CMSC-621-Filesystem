import os 
import sys
import json
import pika

nodename = os.environ['RABBITMQ_NODENAME']  # This node's name.

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='direct_main', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='direct_main', queue=queue_name, routing_key='all_nodes')  # Listen for messages sent to all nodes.
    channel.queue_bind(exchange='direct_main', queue=queue_name, routing_key=nodename)  # Listen for messages sent to this node.


    # Logic goes here.
    def callback(ch, method, properties, body):
        data = json.loads(body)
        if data['event_type'] == 'cluster_join':
            print(' [x] %r joined the cluster' % (data['sender']))
        elif data['event_type'] == 'cluster_leave':
            print(' [x] %r left the cluster' % (data['sender']))
        elif method.routing_key == nodename:
            print(' [x] Received a personal message!')
        else:
            print(' [x] Unhandled message received.')


    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
