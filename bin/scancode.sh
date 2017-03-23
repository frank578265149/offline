#!/usr/bin/env bash

export HADOOP_USER_NAME=hdfs


sudo -u hdfs  spark-submit \
--class bigdata.analysis.scala.core.ScanCode   --master local --driver-memory 2g --executor-memory 1g  \
--driver-class-path /etc/hive/conf.cloudera.hive:$FWDIR/lib/mysql-connector-java-5.1.36.jar --executor-cores 2 --num-executors 2  --queue default \
$FWDIR/lib/tracing-source-1.0-SNAPSHOT-jar-with-dependencies.jar  --prize-log  /home/frank/IdeaProjects/tracing-source/log/prize  \
--user-log  /home/frank/IdeaProjects/tracing-source/log/user