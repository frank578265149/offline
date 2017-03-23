#!/usr/bin/env bash

export PIO_HOME="$(cd `dirname $0`/..; pwd)"

export HADOOP_USER_NAME=hdfs

. ${PIO_HOME}/bin/load-pio-env.sh

echo "+++++++++++++++++++create hbase table  begin +++++++++++++++++++++ "
sh  ${FWDIR}/bin/hbase_create_table.sh
if [ $? -eq 0 ]
then
    echo  "success create hbase table!!!"
else
    echo  "fail create hbase table   !!!"
	exit -1
fi
echo "+++++++++++++++++++create hbase table begin +++++++++++++++++++++ "
echo "Starting XMHT Event Server>>>>>>>>>>>>>>>>>>>>."
${PIO_HOME}/bin/pio-daemon.sh  ${PIO_HOME}/eventserver.pid   run  --table-name $1 --manifest  ${PIO_HOME}/conf/engine.json  -- --master  yarn --deploy-mode cluster    --driver-memory  2G
