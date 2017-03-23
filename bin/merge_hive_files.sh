#!/usr/bin/env bash

FWDIR="$(cd `dirname $0`/..; pwd)"

hive -f    ${FWDIR}/bin/prizeTable.sql

yes_val=`date +"%Y%m%d" -d "-1day"`
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
hive -e "set mapred.reduce.tasks = 1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into prize_details partition(ee,dt) select serviceId,logTime,logid,
eid,ecode,ename,prizeOrderId,barcode,activityId,activityName,prizeAmount,
prizeIntegration,productId,productName,prizeType,prizeTypeName,isType,isTypeName,
userId,userName,openId,isStatus,isStatusName,isPrize,isPrizeName,prizeId,prizeName,
createTime,acceptTime,getTime,isFirstGet,isFirstGetName,payOpenId,redBagType,redBagTypeName,
isActive,isActiveName,dealerId,dealerName,shopId,shopName,tyopenId,ecode as ee,
from_unixtime(unix_timestamp(createTime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') as dt from prize_details_tmp
where dt='${yes_val}' "

hive -e "set mapred.reduce.tasks = 1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table prize_details partition(ee,dt) select serviceId,logTime,logid,
eid,ecode,ename,prizeOrderId,barcode,activityId,activityName,prizeAmount,
prizeIntegration,productId,productName,prizeType,prizeTypeName,isType,isTypeName,
userId,userName,openId,isStatus,isStatusName,isPrize,isPrizeName,prizeId,prizeName,
createTime,acceptTime,getTime,isFirstGet,isFirstGetName,payOpenId,redBagType,redBagTypeName,
isActive,isActiveName,dealerId,dealerName,shopId,shopName,tyopenId,ee,dt from prize_details
"


#<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>hive>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>



hive -e "set mapred.reduce.tasks = 1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into prize_number_details_tmp partition(ee,dt) select serviceId,logTime,logid,eid,ecode,ename,scan_code_order_id,
scan_time_prize_id,scan_time_prize_name,user_id,status,status_name,use_scan_time,scan_time_prize_type,scan_time_prize_amount,
name,tel,address,post_code,create_time,update_time,duijiang_status,tyopenId,ecode as ee,
from_unixtime(unix_timestamp(createTime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') as dt from  prize_number_details_tmp
where dt='${yes_val}'"


hive -e "set mapred.reduce.tasks = 1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  prize_number_details partition(ee,dt) select serviceId,logTime,logid,eid,ecode,ename,scan_code_order_id,
scan_time_prize_id,scan_time_prize_name,user_id,status,status_name,use_scan_time,scan_time_prize_type,scan_time_prize_amount,
name,tel,address,post_code,create_time,update_time,duijiang_status,tyopenId,ee,dt from  prize_number_details"


