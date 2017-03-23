package bigdata.analysis.scala.service


import bigdata.analysis.scala.dao.jdbc._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created by frank on 16-10-12.
  */
abstract  class JDBCService  extends  Serializable{
  def saveOrUpdateScanCodeCount(scanCodeCount: ScanCodeCount):Int
  def saveOrUpdateDrawPrizeSum(drawPrizeSum: DrawPrizeSum):Int
  def saveOrUpdateConvertPrizeSum(convertPrizeSum: ConvertPrizeSum):Int
  def saveOrUpdateShopPrizeSum(shopPrizeSum: ShopPrizeSum):Int
  def saveOrUpdateAccountSum(accountsSum: AccountsSum):Int
  def saveOrUpdateCountPrizeSum(countPrizeSum: CountPrizeSum):Int
  def saveOrUpdateStatisticsSum(activityCount: ActivityCount):Int
  def saveOrUpdateStatisticsCount(statisticsCount: StatisticsCount):Int
  def prizeDetailsToMysql(rdd:RDD[Row]):Int
  def drawNumberDetailsToMysql(rdd:RDD[Row]):Int
  def accountsDetailsToMysql(rdd:RDD[Row]):Int
  def isDuplicateLog(key:String):Boolean
  def saveOrUpdateTracingCore(tracingCore: TracingCoreBean):Int
  def saveOrUpdateDealerPrizeSum(dealerPrizeSum: DealerPrizeSum):Int
}
