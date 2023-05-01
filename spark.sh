#!/bin/bash

~/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--master yarn \
--deploy-mode client \
--conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.eventLog.dir=/home/nomad/spark/logs" \
--num-executors 1 \
--executor-cores 1 \
--py-files ~/projects/comment_analyzer/packages.zip \
~/projects/comment_analyzer/spanemo/spark.py
