# Set base image to RabbitMQ.
FROM rabbitmq:3

MAINTAINER Andre Nguyen, Christian Badolato

RUN apt-get update

# Install Packages
RUN apt-get install -y python3-pip
RUN apt-get install -y libfuse-dev

# Install FUSEpy
RUN pip3 install fusepy

# Create filesystem directories
RUN mkdir -p /filesystem
RUN mkdir -p /mountpoint

# Instead of copying files, I am using a bind mount. See docker-compose.yml.
WORKDIR /app

# Run hello world script on container start.
CMD ["python3", "src/loopback.py", "/filesystem", "/mountpoint"]
