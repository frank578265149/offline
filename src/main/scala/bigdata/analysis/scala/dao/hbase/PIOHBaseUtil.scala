package org.apache.hadoop.hbase.mapreduce

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;

/**
  * Created by frank on 16-12-26.
  */

object PIOHBaseUtil {

  def convertScanToString(scan: Scan): String = {
    /*TableMapReduceUtil.convertScanToString(scan)*/
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray())
  }
}
