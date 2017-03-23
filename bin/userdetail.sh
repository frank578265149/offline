#!/usr/bin/env bash
export USEE_HOME="$(cd `dirname $0`/..; pwd)"
echo "${USEE_HOME}"
sudo -u hdfs spark-submit --master  local[2] \
--class bigdata.analysis.scala.core.UserStreamDetail \
--files ${USEE_HOME}/conf/log4j.properties \
--driver-java-options "-Dlog4j.configuration=file://${USEE_HOME}/conf/log4j.properties" \
--driver-class-path  /etc/hive/conf.cloudera.hive:${USEE_HOME}/lib/commons-dbcp2-2.1.1.jar:${USEE_HOME}/lib/commons-pool2-2.4.2.jar:${USEE_HOME}/lib/ezmorph-1.0.6.jar:${USEE_HOME}/lib/grizzled-slf4j_2.10-1.0.2.jar:${USEE_HOME}/lib/joda-convert-1.6.jar:${USEE_HOME}/lib/joda-time-2.3.jar:${USEE_HOME}/lib/paranamer-2.6.jar:${USEE_HOME}/lib/scopt_2.10-3.3.0.jar:${USEE_HOME}/lib/slf4j-api-1.7.7.jar:${USEE_HOME}/lib/slf4j-log4j12-1.7.10.jar:${USEE_HOME}/lib/scalikejdbc_2.10-2.3.2.jar:${USEE_HOME}/lib/scalikejdbc-core_2.10-2.3.2.jar:${USEE_HOME}/lib/scalikejdbc-interpolation_2.10-2.3.2.jar:${USEE_HOME}/lib/scalikejdbc-interpolation-macro_2.10-2.3.2.jar:${USEE_HOME}/lib/mysql-connector-java-5.1.36.jar \
--jars ${USEE_HOME}/lib/mysql-connector-java-5.1.36.jar,${USEE_HOME}/lib/log4j-1.2.17.jar,${USEE_HOME}/lib/grizzled-slf4j_2.10-1.0.2.jar,${USEE_HOME}/lib/scopt_2.10-3.3.0.jar,${USEE_HOME}/lib/scalikejdbc_2.10-2.3.2.jar,${USEE_HOME}/lib/commons-pool2-2.4.2.jar,${USEE_HOME}/lib/commons-dbcp2-2.1.1.jar,${USEE_HOME}/lib/joda-time-2.3.jar,${USEE_HOME}/lib/json4s-ast_2.10-3.2.10.jar,${USEE_HOME}/lib/json4s-core_2.10-3.2.10.jar,${USEE_HOME}/lib/json4s-ext_2.10-3.2.10.jar,${USEE_HOME}/lib/json4s-native_2.10-3.2.10.jar,${USEE_HOME}/lib/scalikejdbc-interpolation_2.10-2.3.2.jar,${USEE_HOME}/lib/scalikejdbc-core_2.10-2.3.2.jar,${USEE_HOME}/lib/scalikejdbc-interpolation-macro_2.10-2.3.2.jar \
 --driver-memory 1g ${USEE_HOME}/lib/stream-statistics-1.0-SNAPSHOT.jar
