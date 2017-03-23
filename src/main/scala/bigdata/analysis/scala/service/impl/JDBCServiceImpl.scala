package bigdata.analysis.scala.service.impl

import bigdata.analysis.scala.dao.jdbc._
import grizzled.slf4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import bigdata.analysis.scala.service.JDBCService

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by frank on 16-10-12.
  */
class JDBCServiceImpl (serviceName: String = "JDBC") extends JDBCService {
  @transient
  protected lazy val logger = Logger[this.type]

  override def saveOrUpdateDrawPrizeSum (drawPrizeSum: DrawPrizeSum): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateDrawPrizeSum(drawPrizeSum)
    f onFailure {
      case t => logger.error("Insert to mysql table  ----->drawPrizeSum failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert to mysql table  ---->drawPrizeSum  successful id:${a}")
    }
    1
  }
  override def saveOrUpdateDealerPrizeSum(dealerPrizeSum: DealerPrizeSum): Int = {
    val f=JDBCTool.mysqlDao.saveOrUpdateDealerPrizeSum(dealerPrizeSum)
    1
  }
  override def prizeDetailsToMysql (rdd: RDD[Row]): Int = {
    rdd.foreach { row =>
      val as = row.toSeq
      val f = JDBCTool.mysqlDao.insertPrizeDetails(as)
      f onFailure {
        case t => logger.error("Insert mysql table ---->> prizeDetails failure: " + t.getMessage)
      }
      f onSuccess {
        case a => logger.info(s"Insert mysql table ---->> prizeDetails  successful id:${a}")
      }
    }
    1
  }

  override def accountsDetailsToMysql (rdd: RDD[Row]): Int = {
    rdd.foreach { row =>
      val as = row.toSeq
      val f = JDBCTool.mysqlDao.insertAccountsDetails(as)
      f onFailure {
        case t => logger.info("Insert to mysql table ------>> AcountDetailsToMysql failure: " + t.getMessage)
      }
      f onSuccess {
        case a => logger.info(s"Insert to mysql table ------>> AcountDetailsToMysql successful id:${a}")
      }
    }
    1
  }


  override def saveOrUpdateAccountSum (accountsSum: AccountsSum): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateAccountsSum(accountsSum)
    f onFailure {
      case t => logger.error("Insert to mysql table accountSum  failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert to mysql table ------>> accountSum successful id:${a}")
    }
    1
  }


  override def saveOrUpdateShopPrizeSum (shopPrizeSum: ShopPrizeSum): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateShopPrizeSum(shopPrizeSum)
    f onFailure {
      case t => logger.error("Insert mysql table ------>> shopPrizeSum  failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert mysql table ------>> shopPrizeSum successful id:${a}")
    }
    1
  }

  override def saveOrUpdateScanCodeCount(scanCodeCount: ScanCodeCount):Int={
    JDBCTool.mysqlDao.saveOrUpdateScanCodeCount(scanCodeCount)
    1
  }
  override def saveOrUpdateStatisticsSum (activityCount: ActivityCount): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateStatisticsSum(activityCount)
    1
  }
  override def saveOrUpdateStatisticsCount (statisticsCount: StatisticsCount): Int = {
    JDBCTool.mysqlDao.saveOrUpdateStatisticsCount(statisticsCount)
    1
  }



  override def saveOrUpdateConvertPrizeSum (convertPrizeSum: ConvertPrizeSum): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateConvertPrizeSum(convertPrizeSum)
    f onFailure {
      case t => logger.error("Insert mysql table ---->>  convertPrizeSum failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert mysql table ---->> convertPrizeSum   successful id:${a}")
    }
    1
  }

  override def saveOrUpdateTracingCore (tracingCore: TracingCoreBean): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateTracingCore(tracingCore)
    f onFailure {
      case t => logger.error("Insert mysql table ---->>  tracingCore failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert mysql table ---->> tracingCore   successful id:${a}")
    }


    1
  }

  override def saveOrUpdateCountPrizeSum (countPrizeSum: CountPrizeSum): Int = {
    val f = JDBCTool.mysqlDao.saveOrUpdateCountPrizeSum(countPrizeSum)
    f onFailure {
      case t => logger.error("Insert mysql table ---->>  countPrizeSum failure: " + t.getMessage)
    }
    f onSuccess {
      case a => logger.info(s"Insert mysql table ---->> countPrizeSum   successful id:${a}")
    }
    1
  }

  override def drawNumberDetailsToMysql (rdd: RDD[Row]): Int = {
    rdd.foreach { row =>
      val as = row.toSeq
      val f = JDBCTool.mysqlDao.insertCountPrizeDetails(as)
      f onFailure {
        case t => logger.info("Insert to mysql table ------>> drawNumberDetailsToMysql failure: " + t.getMessage)
      }
      f onSuccess {
        case a => logger.info(s"Insert to mysql table ------>> drawNumberDetailsToMysql successful id:${a}")
      }
    }
    1
  }

  override def isDuplicateLog (key: String): Boolean = {
    JDBCTool.mysqlDao.isDuplicateLog(key)
  }
}
