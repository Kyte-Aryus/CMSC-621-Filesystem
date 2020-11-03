# CMSC-621-Filesystem

cbad1@umbc.edu  
tv28617@umbc.edu  

## Container setup instructions.

1. Install Docker and Docker Compose.
2. Build the image using `docker build -t fsnode .` 
3. Start the cluster containers with `docker-compose up -d`
4. View logs with `docker-compose logs -f`
5. Open a container CLI using the Docker Dashboard.
6. Set up a receiver on a node using `python3 src/listen.py`
7. On another node, send a message using one of `python3 src/example_send_node0.py`, `python3 src/on_join.py`, or `python3 src/on_leave.py`, which send a message to node 0, all nodes informing of a join, or all nodes informing of a leave respectively.
8. View cluster metrics at `http://localhost:15672` (guest/guest).
9. Shutdown the cluster with `docker-compose down`

## Current filesystem status
On startup, the nodes will run `src/loopback.py` passing in the directories `/filesystem/` and `/mountpoint/`. Currently a loopback filesystem is implemented, `/filesystem/` is mounted onto `/mountpoint/` and all changes in `/mountpoint/` will be reflected in `/filesystem/` (the reverse is not true). This will allow for modification of files in `/mountpoint/` to perform validation and communication logic prior to commiting their changes to `/filesystem/`.
`debug_log.txt` shows all of the recorded events in the mounted filesystem. Each of these events can be hooked into using FUSE within `src/loopback.py`. To see how this works, but a custom print statement in a function of `src/loopback.py` and rebuild the docker containers. The function will execute according to FUSE and will run the new custom logic. 

## Notes.

On container start, `src/loopback.py` is run. For some reason, detailed node statistics are not showing up on the manager monitor. 
