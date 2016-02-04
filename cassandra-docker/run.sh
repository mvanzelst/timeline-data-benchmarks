#!/bin/bash
sudo docker run --name timeseries-benchmark-cassandra -i -t -v /media/marijn/data/cassandra-timeseries-benchmark:/var/lib/cassandra timeseries-benchmark/cassandra
