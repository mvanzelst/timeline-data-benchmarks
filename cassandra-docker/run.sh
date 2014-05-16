#!/bin/bash
sudo docker run --name cass1 -d -v `realpath cassandra-1`:/etc/cassandra -v /tmp/cassandra-1-storage:/storage iot-benchmark/cassandra /usr/sbin/cassandra -f
sleep 1
../pipework/pipework br1 cass1 192.168.148.1/24

sudo docker run --name cass2 -d -v `realpath cassandra-2`:/etc/cassandra -v /tmp/cassandra-2-storage:/storage iot-benchmark/cassandra /usr/sbin/cassandra -f
sleep 1
../pipework/pipework br1 cass2 192.168.148.2/24

sudo docker run --name cass3 -d -v `realpath cassandra-3`:/etc/cassandra -v /tmp/cassandra-3-storage:/storage iot-benchmark/cassandra /usr/sbin/cassandra -f
sleep 1
../pipework/pipework br1 cass3 192.168.148.3/24


#ip addr add 192.168.148.254/24 dev br1
