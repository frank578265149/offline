#!/usr/bin/env bash


# Declaring Variables
table0="prize_log_details"
table1="account_details"
table2="draw_number_details"
table3="log_meta"
table4="hbase_test"
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Creating a prize_log_details table in HBase

#echo "exists '$table0'" | hbase shell > log
#cat log | grep "Table prize_log_details does exist"
#if [ $? = 0 ];then
 #   echo "************  table prize_log_details is already exists **********"

# Either you can use truncate or disable & drop options

#   echo "disable '$table0'" | hbase shell
#   echo "drop '$table0'" | hbase shell
#   echo "truncate '$table0'" | hbase shell
#   echo "create '$table0','count'" | hbase shell

#else
#    echo "***********  need to create a table  prize_log_details **********"
#    echo "create '$table0','abbr'" | hbase shell
#fi
#cat /dev/null  > log
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Creating a account_details table in HBase

#echo "exists '$table1'" | hbase shell > log
#cat log | grep "Table account_details does exist"
#if [ $? = 0 ];then
#    echo "************  table account_details is already exists **********"

# Either you can use truncate or disable & drop options

#   echo "disable '$table0'" | hbase shell
#   echo "drop '$table0'" | hbase shell
#   echo "truncate '$table0'" | hbase shell
#   echo "create '$table0','count'" | hbase shell

#else
#    echo "***********  need to create a table  account_details **********"
#    echo "create '$table0','abbr'" | hbase shell
#fi
#cat /dev/null  > log

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Creating a draw_number_details table in HBase

#echo "exists '$table2'" | hbase shell > log
#cat log | grep "Table draw_number_details does exist"
#if [ $? = 0 ];then
#    echo "************  table draw_number_details is already exists **********"

# Either you can use truncate or disable & drop options

#   echo "disable '$table0'" | hbase shell
#   echo "drop '$table0'" | hbase shell
#   echo "truncate '$table0'" | hbase shell
#   echo "create '$table0','count'" | hbase shell

#else
#    echo "***********  need to create a table  draw_number_details **********"
#    echo "create '$table2','abbr'" | hbase shell
#fi
#cat /dev/null  > log


#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Creating a log_meta table in HBase

#echo "exists '$table3'" | hbase shell > log
#cat log | grep "Table log_meta does exist"
#if [ $? = 0 ];then
#    echo "************  table log_meta is already exists **********"

# Either you can use truncate or disable & drop options

#   echo "disable '$table0'" | hbase shell
#   echo "drop '$table0'" | hbase shell
#   echo "truncate '$table0'" | hbase shell
#   echo "create '$table0','count'" | hbase shell

#else
#    echo "***********  need to create a table  log_meta **********"
#    echo "create '$table3','count'" | hbase shell
#fi
#cat /dev/null  > log
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Creating a log_meta table in HBase

echo "exists '$table4'" | hbase shell > log
cat log | grep "Table hbase_test does exist"
if [ $? = 0 ];then
    echo "************  table hbase_test is already exists **********"

# Either you can use truncate or disable & drop options

#   echo "disable '$table0'" | hbase shell
#   echo "drop '$table0'" | hbase shell
#   echo "truncate '$table0'" | hbase shell
#   echo "create '$table0','count'" | hbase shell

else
    echo "***********  need to create a table  hbase_test **********"
    echo "create '$table4','count'" | hbase shell
fi
cat /dev/null  > log


exit