#!/bin/bash

set -ev

echo $DOCKER_COMPOSE_VERSION

sudo rm -f /usr/local/bin/docker-compose

curl -L https://github.com/docker/compose/releases/download/$DOCKER_COMPOSE_VERSION/docker-compose-`uname -s`-`uname -m` > docker-compose

chmod +x docker-compose

ls -l

sudo mv docker-compose /usr/local/bin

docker-compose --version
