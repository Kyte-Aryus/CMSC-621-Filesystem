# CMSC-621-Filesystem

cbad1@umbc.edu  
tv28617@umbc.edu  

## Container setup instructions.

1. Install Docker and Docker Compose.
2. Build the image using `docker build -t fsnode .` 
3. Start the cluster containers with `docker-compose up -d`
4. View logs with `docker-compose logs -f`
5. Open a container CLI using the Docker Dashboard.
6. Add a node to the cluster using `sh join_cluster.sh`
7. Set up a receiver on a node using `python3 src/receiver.py`
8. On another node, send a message using `python3 src/sender.py`
9. View cluster metrics at `http://localhost:15672`
10. Shutdown the cluster with `docker-compose down`

## Current filesystem status
On startup, the nodes will run `src/loopback.py` passing in the directories `/filesystem/` and `/mountpoint/`. Currently a loopback filesystem is implemented, `/filesystem/` is mounted onto `/mountpoint/` and all changes in `/mountpoint/` will be reflected in `/filesystem/` (the reverse is not true). This will allow for modification of files in `/mountpoint/` to perform validation and communication logic prior to commiting their changes to `/filesystem/`.
`debug_log.txt` shows all of the recorded events in the mounted filesystem. Each of these events can be hooked into using FUSE within `src/loopback.py`. To see how this works, but a custom print statement in a function of `src/loopback.py` and rebuild the docker containers. The function will execute according to FUSE and will run the new custom logic. 

## Notes.

Right now, a three node cluster will be created. All the containers in the cluster are automatically attached to a same network. I also have a RabbitMQ manager node start for monitoring. Note that the manager node is not needed for the filesystem to run, it is just used for monitoring. I'm using a bind mount so that the code in the containers is always synced with whatever the code is in the repo on the host. The nodes are based on the RabbitMQ image. On container start, `src/loopback.py` is run.   

A couple of things we will need to change at somepoint:  

1. The cluster join script assumes that the manager is always available. Need to switch to use peer discovery and make automatic: https://www.rabbitmq.com/cluster-formation.html
2. For some reason, detailed node statistics are not showing up on the manager monitor. 
3. For replication to occur, we need to use a queue type that supports replication: https://www.rabbitmq.com/clustering.html#cluster-membership 
