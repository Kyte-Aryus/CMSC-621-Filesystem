rabbitmq-plugins enable rabbitmq_management_agent
rabbitmq-server -detached

python3 src/CABNfs.py /filesystem /mountpoint 3