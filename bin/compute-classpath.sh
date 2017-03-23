#!/usr/bin/env bash

SCALA_VERSION=2.10

#Figure out where XMHT is installed
FWDIR="$(cd `dirname $0`/..; pwd)"

. ${FWDIR}/bin/load-pio-env.sh

#Build up classpath
CLASSPATH="${FWDIR}/conf"


ASSEMBLY_DIR="${FWDIR}/target"

if [ -n "$JAVA_HOME" ]; then
  JAR_CMD="$JAVA_HOME/bin/jar"
else
  JAR_CMD="jar"
fi

#Use xmht-assembly JAR from either RELEASE  or assembly directory
if [ -f "${FWDIR}/RELEASE" ]; then
  assembly_folder="${FWDIR}"/lib
else
  assembly_folder="${ASSEMBLY_DIR}"
fi

ASSEMBLY_JAR=$(ls "${assembly_folder}"/tracing-source-1.0-SNAPSHOT-jar-with-dependencies.jar 2>/dev/null)
#echo "${ASSEMBLY_JAR}"
CLASSPATH="$CLASSPATH:${ASSEMBLY_JAR}"

# Add hadoop conf dir if given -- otherwise FileSystem.*, etc fail ! Note, this
# assumes that there is either a HADOOP_CONF_DIR or YARN_CONF_DIR which hosts
# the configurtion files.
if [ -n "$HADOOP_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$HADOOP_CONF_DIR"
fi
if [ -n "$YARN_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$YARN_CONF_DIR"
fi
if [ -n "$HBASE_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$HBASE_CONF_DIR"
fi
if [ -n "$ES_CONF_DIR" ]; then
  CLASSPATH="$CLASSPATH:$ES_CONF_DIR"
fi
if [ -n "$POSTGRES_JDBC_DRIVER" ]; then
  CLASSPATH="$CLASSPATH:$POSTGRES_JDBC_DRIVER"
fi
if [ -n "$MYSQL_JDBC_DRIVER" ]; then
  CLASSPATH="$CLASSPATH:$MYSQL_JDBC_DRIVER"
fi

echo "$CLASSPATH"