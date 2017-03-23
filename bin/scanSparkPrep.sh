#!/usr/bin/env bash


export HADOOP_USER_NAME=hdfs

source /etc/profile
echo "create 'scan_details',{NAME => 'f',COMPRESSION => 'snappy'}"|hbase shell
#echo "create 'tmp_hbase_scan_details',{NAME => 'f',COMPRESSION => 'snappy'}"|hbase shell

#/opt/cloudera/parcels/CDH-5.4.3-1.cdh5.4.3.p0.6/lib/hive<<EOF
hive<<EOF 
#drop table  IF EXISTS scan_details;
#hive tmp table  date=Nowdate
create external table tmp_scan_details(
serviceId string,
logTime string,
logid string,
eid string,
ecode string,
ename string,
barcode string,
barcodeType string,
productId string,
productName string,
scanTime string,
lng string,
lat string,
type string,
phoneInfo string,
openId string,
ipaddress string,
codeStatus string,
isFirst string,
activityId string,
activityName string,
tyopenId string,
useragent string)partitioned by (ecodeId string,date string)
STORED AS PARQUET
location '/xmht/hive/tmp_scan_details';

#hive table
create external table scan_details(
serviceId string,
logTime string,
logid string,
eid string,
ecode string,
ename string,
barcode string,
barcodeType string,
productId string,
productName string,
scanTime string,
lng string,
lat string,
type string,
phoneInfo string,
openId string,
ipaddress string,
codeStatus string,
isFirst string,
activityId string,
activityName string,
tyopenId string,
useragent string)partitioned by (ecodeId string,date string)
STORED AS PARQUET
location '/xmht/hive/scan_details';

##hive distinct  can be resolved through the data warehouse
#CREATE EXTERNAL TABLE tmp_hbase_scan_details(
#serviceId string,
#logTime string,
#logid string,
#eid string,
#ecode string,
#ename string,
#barcode string,
#barcodeType string,
#productId string,
#productName string,
#scanTime string,
#lng string,
#lat string,
#type string,
#phoneInfo string,
#openId string,
#ipaddress string,
#codeStatus string,
#isFirst string,
#activityId string,
#activityName string,
#tyopenId string,
#useragent string)
#ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
#STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
#WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,f:logTime,f:logid,f:eid,f:ecode,f:ename,f:barcode,f:barcodeType,f:productId,f:productName,f:scanTime,f:lng,f:lat,f:type,f:phoneInfo,f:openId,f:ipaddress,f:codeStatus,f:isFirst,f:activityId,f:activityName,f:tyopenId,f:useragent")
#TBLPROPERTIES("hbase.table.name" = "tmp_hbase_scan_details");

EOF
exit;





