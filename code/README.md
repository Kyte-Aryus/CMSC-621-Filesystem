# CMSC-621-Filesystem

cbad1@umbc.edu  
tv28617@umbc.edu  

## Instructions.

1. Install Docker and Docker Compose.  
2. Build the image using `docker build -t fsnode .`   
3. Start the cluster containers with `docker-compose up -d`  
4. View logs with `docker-compose logs -f`  
5. Open container CLIs using the Docker Dashboard.  
6. Run commands in `start.sh` to start the server.  
7. `/filesystem/` is where local data is stored. File operations however should operate on `/mountpoint/`. This allows for modification of files in `/mountpoint/` to perform validation and communication logic prior to changes to `/filesystem/`. `debug_log.txt` shows the recorded events in the mounted filesystem. These events are hooked into using FUSE, as implemented by `src/CABNfs.py`  
8. View cluster status at `http://localhost:15672` (guest/guest). For some reason, detailed node statistics are not showing up on the manager monitor.   
9. Shutdown a node (but not the container) using `shutdown.py` with the parameter of node number (starting at '0')  
10. Shutdown the cluster with `docker-compose down`   
11. Client simulation and metrics analysis in `tests/`  
