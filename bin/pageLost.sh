#!/usr/bin/env bash



FWDIR="$(cd `dirname $0`/..; pwd)"


export HADOOP_USER_NAME=hdfs



#`spark-submit --class bigdata.analysis.java.ScanMain  --master spark://centos14:7077 --executor-memory 512m --total-executor-cores 1 $FWDIR/lib/stream-statistics-1.0-SNAPSHOT-jar-with-dependencies.jar`
`spark-submit --class bigdata.analysis.scala.core.PageLost  --master yarn-cluster --driver-memory 2g --executor-memory 1g   --executor-cores 2 --num-executors 2  --queue default $FWDIR/lib/tracing-source-1.0-SNAPSHOT-jar-with-dependencies.jar &`
#`spark-submit --class bigdata.analysis.java.ScanMain  --master yarn-cluster --driver-memory 1g --executor-memory 1g   --executor-cores 2 --num-executors 2  --queue default  $FWDIR/lib/stream-statistics-1.0-SNAPSHOT-jar-with-dependencies.jar`
