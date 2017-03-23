#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs
export USEE_HOME="$(cd `dirname $0`/..; pwd)"
echo "${USEE_HOME}"
source /etc/profile
echo "create 'base_product',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create hbase base_product complete 1 "
echo "create 'base_product_property',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create base_product_property complete 2"
echo "create 'stock_circulation',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create stock_circulation complete 3"
echo "create 'cha_channel',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create cha_channel complete 4"
echo "create 'ent_sales_area',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create ent_sales_area complete 5"
echo "create 'ent_area_sales_area_rela',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create ent_area_sales_area_rela 6"