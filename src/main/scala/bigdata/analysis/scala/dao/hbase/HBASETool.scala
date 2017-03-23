package bigdata.analysis.scala.dao.hbase

/**
  * Created by frank on 16-11-26.
  */
object HBASETool{

  val hbaseProxy=getHbaseProxy()
  def getHbaseProxy():HbaseAdapter={
    // init hbase connection pool
    val hbaseAdapter:HbaseAdapter=HbaseAdapter.getInstance()
    hbaseAdapter.init();
    hbaseAdapter
  }
  // use hbase Adapter function
  //hbaseAdapter.isExist("table");
  def rowKeyExist(rowKey:String):Boolean={
    val tableName="log_meta"
    val result=hbaseProxy.getRowByRowKey(tableName,rowKey)
    if(result.isEmpty){
      true
    }else{
      false
    }
  }
  def rowKeyInsert(rowKey:String,value:String):Int={
    val tableName="log_meta"
    hbaseProxy.putRowByRowKey(tableName,rowKey,"count","log",value)
  }
  def filterLog( key:String,value:String): Boolean ={
    try{
      if(rowKeyExist(key)){
        rowKeyInsert(key,value)
        true
      }else{
        false
      }
    }catch {
      case e:Exception=>
        true
    }

  }

}
