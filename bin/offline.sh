#!/usr/bin/env bash

export XMHT_HOME="$(cd `dirname $0`/..; pwd)"

export HADOOP_USER_NAME=hdfs

. ${XMHT_HOME}/bin/load-pio-env.sh

java  -cp offline-1.0-SNAPSHOT-jar-with-dependencies.jar  bigdata.analysis.scala.core.SparkSubmit --verbose  --properties-file ../conf/conf.properties