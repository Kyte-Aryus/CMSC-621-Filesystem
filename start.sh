rabbitmq-plugins enable rabbitmq_management_agent
rabbitmq-server -detached

python3 src/loopback.py /filesystem /mountpoint