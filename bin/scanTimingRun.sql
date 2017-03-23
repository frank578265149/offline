#drop table  IF EXISTS scan_details;set hive.exec.dynamic.partition=true;set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.dynamic.partitions.pernode=50000;set hive.exec.dynamic.partitions.partitions=50000;set hive.exec.max.created.files=500000;set hive.merge.mapfiles=true;set mapred.reduce.tasks =1;###hbase distinct#insert into tmp_hbase_scan_details#select serviceId,logTime,logid,eid,ecode,ename,barcode,barcodeType,productId,productName,scanTime,lng,lat,type,phoneInfo,openId,ipaddress,codeStatus,isFirst,activityId,activityName,tyopenId,useragent#from tmp_scan_details where date=\'$oldDate\';#hive  ecode/date(day)insert into scan_details partition(ecodeId,date)select serviceId,logTime,logid,eid,ecode,ename,barcode,barcodeType,productId,productName,scanTime,lng,lat,type,phoneInfo,openId,ipaddress,codeStatus,isFirst,activityId,activityName,tyopenId,useragent,ecode as ecodeId, substring(scanTime,1,10) as date from tmp_scan_details where date= date_sub ( from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1);