#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs
export USEE_HOME="$(cd `dirname $0`/..; pwd)"
echo "${USEE_HOME}"
source /etc/profile

hadoop fs -mkdir /xmhtfiles
hadoop fs -put file.py /xmhtfiles/
hadoop fs -put PipeScript3.py /xmhtfiles/

echo "create 'user_details',{NAME => 'userFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create hbase user_details complete 1 "
echo "create 'user_error_details',{NAME => 'f',COMPRESSION => 'snappy'}"|hbase shell
echo "create hbase user_error_details complete 1"
echo "create 'user_details',{NAME => 'userFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create hbase user_details complete 2"
echo "create 'user_error_details',{NAME => 'f',COMPRESSION => 'snappy'}"|hbase shell
echo "create hbase user_error_details complete 2"
