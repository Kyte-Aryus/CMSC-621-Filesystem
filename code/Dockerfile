# Set base image to RabbitMQ.
FROM rabbitmq:3.8.8

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

# RabbitMQ plugins.
RUN rabbitmq-plugins enable rabbitmq_management

# Instead of copying files, I am using a bind mount. See docker-compose.yml.
WORKDIR /app

# The RabbitMQ config file needs to be copied.
COPY rabbitmq.conf /etc/rabbitmq/

# Start RabbitMQ server and run Fuse filesystem on container start.
CMD ["tail", "-f", "/dev/null"]
