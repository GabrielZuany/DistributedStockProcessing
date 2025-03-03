#!/bin/bash
set -e

# Install dependencies
sudo yum install -y make
sudo make install-spark
make install-python3

# Start Hadoop NameNode
make start-namenode

exec "$@"
