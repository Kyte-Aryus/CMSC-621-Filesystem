# Set base image to RabbitMQ.
FROM rabbitmq:3

MAINTAINER Andre Nguyen, Christian Badolato

RUN apt-get update

# Install packages.
RUN apt-get install -y python3-pip
RUN apt-get install -y libfuse-dev
RUN apt-get install -y nano

# Install FUSEpy and pika.
RUN pip3 install fusepy
RUN pip3 install pika

# Create filesystem directories.
RUN mkdir -p /filesystem
RUN mkdir -p /mountpoint

# Instead of copying files, I am using a bind mount. See docker-compose.yml.
WORKDIR /app

# Start RabbitMQ server and run Fuse filesystem on container start.
CMD ["sh", "-c", "chmod +x ./start.sh && ./start.sh"]
