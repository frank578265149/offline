#!/usr/bin/env bash
FWDIR="$(cd `dirname $0`/..; pwd)"


export HADOOP_USER_NAME=hdfs

spark-submit \
--class bigdata.analysis.scala.core.ConsumerProfile   --master local --driver-memory 2g --executor-memory 1g  \
--driver-class-path $FWDIR/lib/mysql-connector-java-5.1.36.jar --executor-cores 2 --num-executors 2  --queue default \
$FWDIR/lib/tracing-source-1.0-SNAPSHOT-jar-with-dependencies.jar
