# CMSC-621-Filesystem

cbad1@umbc.edu  
tv28617@umbc.edu  

## Container setup instructions.

1. Install Docker and Docker Compose.
2. Build the image using `docker build -t fsnode .` 
3. Start the cluster containers with `docker-compose up -d`
4. View logs with `docker-compose logs -f`
5. Open a container CLI using the Docker Dashboard if needed. 
6. Shutdown the cluster with `docker-compose down`

## Current filesystem status
On startup, the nodes will run `src/loopback.py` passing in the directories `/filesystem/` and `/mountpoint/`. Currently a loopback filesystem is implemented, `/filesystem/` is mounted onto `/mountpoint/` and all changes in `/mountpoint/` will be reflected in `/filesystem/` (the reverse is not true). This will allow for modification of files in `/mountpoint/` to perform validation and communication logic prior to commiting their changes to `/filesystem/`.
`debug_log.txt` shows all of the recorded events in the mounted filesystem. Each of these events can be hooked into using FUSE within `src/loopback.py`. To see how this works, but a custom print statement in a function of `src/loopback.py` and rebuild the docker containers. The function will execute according to FUSE and will run the new custom logic. 

## Notes.

Right now, a three node cluster will be created. All the containers in the cluster are automatically attached to a same network. I also have a RabbitMQ manager node start for monitoring. I still need to configure RabbitMQ.  I'm using a bind mount so that the code in the containers is always synced with whatever the code is in the repo on the host. The nodes are based on the RabbitMQ image, plus an install of `python3` and `pip3`. On container start, `src/loopback.py` is run. The exposed ports and the environment variables set in `docker-compose.yml` are not used currently. 
