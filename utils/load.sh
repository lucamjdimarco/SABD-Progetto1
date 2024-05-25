#!/bin/bash

docker cp ../spark/query1.py spark-master:/opt/spark/work-dir/query1.py
docker cp ../spark/query2.py spark-master:/opt/spark/work-dir/query2.py
docker cp ../spark/query3.py spark-master:/opt/spark/work-dir/query3.py