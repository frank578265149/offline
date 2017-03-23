#!/bin/bash
export SPARK_HOME=/opt/cloudera/parcels/CDH-5.7.2-1.cdh5.7.2.p0.18/lib/spark
export HIVE_CONF_DIR=/opt/cloudera/parcels/CDH-5.7.2-1.cdh5.7.2.p0.18/lib/hive/conf
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-5.7.2-1.cdh5.7.2.p0.18/lib/hadoop-0.20-mapreduce/conf
export HBASE_CONF_DIR=/opt/cloudera/parcels/CDH-5.7.2-1.cdh5.7.2.p0.18/lib/hbase/conf




sh scanSpark.sh &> scanSparkStart.log &

sh newuser.sh  &> newuser.log &
sh userdetail.sh  &> userdetail.log &
sh hiveuserstream.sh  &> hiveuserstream.log &
 
sudo -u hdfs sh pio-start-all.sh  &> pio-start-all.log &
sudo -u hdfs sh pio-start-all-1.sh  &> pio-start-all-1.log &

# sh pio-start-all.sh &> pio-start-all.log &
