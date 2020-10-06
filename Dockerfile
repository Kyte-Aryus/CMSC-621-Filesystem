# Set base image to RabbitMQ.
FROM rabbitmq:3

MAINTAINER Andre Nguyen, Christian Badolato

RUN apt-get update

# Install Python.
RUN apt-get install -y python3-pip

# Instead of copying files, I am using a bind mount. See docker-compose.yml.
WORKDIR /app

# Run hello world script on container start.
CMD ["python3", "src/helloworld.py"]
