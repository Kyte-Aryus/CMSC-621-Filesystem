rabbitmq-server -detached
sleep 180
python3 src/CABNfs.py /filesystem /mountpoint 3