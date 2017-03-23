#!/usr/bin/env bash

export PIO_HOME="$(cd `dirname $0`/..; pwd)"
. ${PIO_HOME}/bin/load-pio-env.sh

#echo "+++++++++++++++++++create hive table  begin +++++++++++++++++++++ "
#hive -f   ${FWDIR}/bin/prizeTable.sql
#if [ $? -eq 0 ]
#then
#    echo  "success create hive table!!!"
#else
#    echo  "fail create hive table   !!!"
#	exit -1
#fi
#echo "+++++++++++++++++++create hive table begin +++++++++++++++++++++ "
#
#echo "Starting XMHT Event Server>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
${PIO_HOME}/bin/pio-daemon.sh  ${PIO_HOME}/eventserver.pid   eventserver   --manifest  ${PIO_HOME}/conf/engine.json  -- --master local
