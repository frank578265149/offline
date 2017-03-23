package bigdata.analysis.scala.dao.jdbc

import java.lang.Integer
import java.text.SimpleDateFormat


import bigdata.analysis.scala.utils.{SendsEmailUtils1, StringUtils}
import grizzled.slf4j.Logger
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
  * Created by frank on 16-9-30.
  */
case class ScanCodeCount (
                           id: String = "",
                           ecode: String = "",
                           year: String = "",
                           month: String = "",
                           hour: String = "",
                           sexNumber: String = "",
                           productId: String = "",
                           productName: String = "",
                           count: Long = 0
                         )

case class DrawPrizeSum (
                          id: String = "drawTableId",
                          ecode: String = "",
                          date: String = "2016-09-23",
                          activity: String = "activity",
                          product: String = "productId",
                          prizeDrawCount: Long = 0,
                          notWinningCount: Long = 0

                        ) extends Serializable

case class ConvertPrizeSum (
                             id: String = "convertTableId",
                             ecode: String = "",
                             date: String = "2016-09-02",
                             activity: String = "",
                             product: String = "productId",
                             prize: String = "12345",
                             countDrawnMode: Long = 0,
                             countPatMode: Long = 0,
                             sumPossessAmount: Double = 0.0,
                             sumPrizeAmount: Double = 0.0

                           ) extends Serializable

case class ShopPrizeSum (
                          id: String = "shopTableId",
                          ecode: String = "",
                          date: String = "2016-09-03",
                          activity: String = "",
                          product: String = "productId",
                          prize: String = "4353",
                          shop: String = "",
                          count_pat_mode: Long = 0
                        ) extends Serializable

case class ActivityCount (
                           id: String = "0",
                           ecode: String = "",
                           typeName: String = "",
                           typeCode: String = "",
                           count: Double = 0.0
                         ) extends Serializable

case class CountPrizeSum (
                           id: String = "CountPrizeSumId",
                           ecode: String = "",
                           date: String = "",
                           prizeId: String = "",
                           countPatMode: Long = 0,
                           prizeAmount: Double = 0.0,
                           redPaperAmount: Double = 0.0

                         ) extends Serializable

case class TracingCoreBean (
                             id: String = "",
                             ecode: String = "",
                             searchDate: String = "",
                             searchArea: String = "",
                             searchProvince: String = "",
                             productCode: String = "",
                             boxNum: Long = 0,
                             bottleNum: Long = 0,
                             ascriptionArea: String = "",
                             ascriptionProvince: String = "",
                             ascriptionAgency: String = "",
                             valid: String = "",
                             property: String = "",
                             sysOrderId: String = "",
                             outTime: String = ""
                           ) extends Serializable

// 对账部分
case class AccountsSum (
                         id: String = "accountId",
                         ecode: String = "",
                         product_id: String = "productId",
                         activity_id: String = "",
                         date: String = "2016-09-03",
                         sum_money: Double = 1,
                         tidy_money: Double = 1,
                         change_money: Double = 1

                       ) extends Serializable

case class StatisticsCount (
                             id: String = "",
                             ecode: String = "",
                             date: String = "",
                             activityId: String = "",
                             source: String = "",
                             provinceId: String = "",
                             cityId: String = "",
                             districtId: String = "",
                             totalScore: Double = 0.0,
                             totalNum: Long = 0
                           )
case class DealerPrizeSum(
                           id:String="dealerTableId",
                           ecode:String="",
                           date:String="2016-09-03",
                           activity:String="",
                           product:String="productId",
                           prize:String="4353",
                           dealer:String="",
                           countPatMode:Long=0,
                           sumPrizeAmount:Double=0.0
                         ) extends  Serializable

// 结束
class MysqlDao (client: String, config: StorageClientConfig, prefix: String) extends Serializable {
  @transient
  lazy protected val logger = Logger[this.type]
  val tableName = "xmht"

  /*  DB  autoCommit{implicit  session=>
      sql"""
      create table if not exists xmht (
        id serial not null primary key,
        name text not null,
        appid integer not null)""".execute().apply()

    }*/
  def saveOrUpdateScanCodeCount (scanCodeCount: ScanCodeCount): String = {
    DB localTx { implicit session =>
      val tablePrefix = scanCodeCount.ecode
      val tableName = s"${tablePrefix}_scan_code_count"
      val command = s" INSERT INTO ${tableName}(id,year,month,hour,sex_number,product_id,product_name," +
        s"count) VALUES('${scanCodeCount.id}','${scanCodeCount.year}','${scanCodeCount.month}'," +
        s"'${scanCodeCount.hour}','${scanCodeCount.sexNumber}','${scanCodeCount.productId}','${scanCodeCount.productName}'," +
        s"${scanCodeCount.count})  ON DUPLICATE KEY UPDATE count=count+${scanCodeCount.count}"
      SQL.apply(command).update().apply()
      ""
    }
  }
    def saveOrUpdateStatisticsCount (statisticsCount: StatisticsCount): String ={
      DB localTx { implicit session =>
        val tablePrefix = statisticsCount.ecode
        val tableName = s"${tablePrefix}_score_produce_count"
        val command = s"INSERT INTO ${tableName}(id,date,activity_id,source,privince,city,county," +
          s"count,person_count) VALUES('${statisticsCount.id}'" +
          s",'${statisticsCount.date}','${statisticsCount.activityId}','${statisticsCount.source}','${statisticsCount.provinceId}',${statisticsCount.cityId},${statisticsCount.districtId}" +
          s",'${statisticsCount.totalScore}','${statisticsCount.totalNum}')  ON DUPLICATE KEY UPDATE " +
          s" count=count+${statisticsCount.totalScore},person_count=person_count+${statisticsCount.totalNum}"
        SQL.apply(command).update().apply()
        statisticsCount.id
      }

    }

    def saveOrUpdateTracingCore (tracingCore: TracingCoreBean): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = tracingCore.ecode
        val tableName = s"${tablePrefix}_tracing_source"
        // val tableName=s"_tracing_source"
        val command = s"INSERT INTO ${tableName}(id,search_date,search_area,search_province,product_code,box_num,bottle_num," +
          s"ascription_area,ascription_province,ascription_agency,valid,property,sys_order_id,outTime) VALUES('${tracingCore.id}'" +
          s",'${tracingCore.searchDate}','${tracingCore.searchArea}','${tracingCore.searchProvince}','${tracingCore.productCode}',${tracingCore.boxNum},${tracingCore.bottleNum}" +
          s",'${tracingCore.ascriptionArea}','${tracingCore.ascriptionProvince}','${tracingCore.ascriptionAgency}','${tracingCore.valid}'" +
          s",'${tracingCore.property}','${tracingCore.sysOrderId}','${tracingCore.outTime}')  ON DUPLICATE KEY UPDATE " +
          s" box_num=box_num+${tracingCore.boxNum},bottle_num=bottle_num+${tracingCore.bottleNum}"
        SQL.apply(command).update().apply()
        tracingCore.id
      }
    }
    def saveOrUpdateDrawPrizeSum (drawPrizeSum: DrawPrizeSum)(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = drawPrizeSum.ecode
        val tableName = s"${tablePrefix}_draw_prize_sum"
        val command = s"INSERT INTO ${tableName}(id,date,activity_id,product_id,prize_draw_count,not_winning_count)" +
          s" VALUES('${drawPrizeSum.id}','${drawPrizeSum.date}',${drawPrizeSum.activity},'${drawPrizeSum.product}',${drawPrizeSum.prizeDrawCount},${drawPrizeSum.notWinningCount})" +
          s" ON DUPLICATE KEY UPDATE prize_draw_count=prize_draw_count+${drawPrizeSum.prizeDrawCount},not_winning_count=not_winning_count+${drawPrizeSum.notWinningCount}"
        SQL.apply(command).update().apply()
        drawPrizeSum.id
      }
    }
  def saveOrUpdateDealerPrizeSum(dealerPrizeSum: DealerPrizeSum):String=
    DB localTx { implicit session =>
      val tablePrefix = dealerPrizeSum.ecode
      val tableName = s"${tablePrefix}_dealer_prize_sum"
      val command = s"INSERT INTO ${tableName}(id,date,activity_id,product_id,prize_id,dealer_id,countPatMode,sumPrizeAmount)" +
        s" VALUES('${dealerPrizeSum.id}','${dealerPrizeSum.date}','${dealerPrizeSum.activity}','${dealerPrizeSum.product}','${dealerPrizeSum.prize}','${dealerPrizeSum.dealer}',${dealerPrizeSum.countPatMode},${dealerPrizeSum.sumPrizeAmount})" +
        s" ON DUPLICATE KEY UPDATE countPatMode =countPatMode+${dealerPrizeSum.countPatMode},sumPrizeAmount=sumPrizeAmount+${dealerPrizeSum.sumPrizeAmount}"
      try{
        SQL.apply(command).update().apply()
        dealerPrizeSum.id
      }catch{
        case e:Exception=>
          val text=s"command:${command}--------cause:${e.getMessage}"
          SendsEmailUtils1.send("error",text,"lilu@sao.so")
          "error"
      }

    }

    def saveOrUpdateStatisticsSum (activityCount: ActivityCount)(implicit ec: ExecutionContext): Long =
      DB localTx { implicit session =>
        val tablePrefix = activityCount.ecode
        val tableName = s"${tablePrefix}_statistics_sum"
        val command = s"INSERT INTO ${tableName}(id,typeName,typeCode,count)" +
          s" VALUES(${activityCount.id},'${activityCount.typeName}','${activityCount.typeCode}',${activityCount.count})" +
          s" ON DUPLICATE KEY UPDATE count=count+${activityCount.count}"
        try {
          SQL.apply(command).update().apply()
        } catch {
          case e: Exception =>
            val text = s"command:${command}----------cause:${e.getMessage}"
            logger.error(text)
            1
        }


        1
      }

    def saveOrUpdateCountPrizeSum (countPrizeSum: CountPrizeSum)(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = countPrizeSum.ecode
        val tableName = s"${tablePrefix}_count_prize_sum"
        val command = s"INSERT INTO ${tableName}(id,date,prize_id,count_pat_mode,prize_amount,red_paper_amount)" +
          s" VALUES('${countPrizeSum.id}','${countPrizeSum.date}','${countPrizeSum.prizeId}',${countPrizeSum.countPatMode},${countPrizeSum.prizeAmount},${countPrizeSum.redPaperAmount})" +
          s" ON DUPLICATE KEY UPDATE count_pat_mode=count_pat_mode+${countPrizeSum.countPatMode},prize_amount=prize_amount+${countPrizeSum.prizeAmount},red_paper_amount=red_paper_amount+${countPrizeSum.redPaperAmount}"
        SQL.apply(command).update().apply()
      }
      countPrizeSum.id
    }
    def saveOrUpdateConvertPrizeSum (convertPrizeSum: ConvertPrizeSum)(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = convertPrizeSum.ecode
        val tableName = s"${tablePrefix}_convert_prize_sum"
        val command = s"INSERT INTO ${tableName}(id,date,activity_id,product_id,prize_id,count_drawn_mode,count_pat_mode,sum_possess_amount,sum_prize_amount)" +
          s" VALUES('${convertPrizeSum.id}','${convertPrizeSum.date}',${convertPrizeSum.activity},'${convertPrizeSum.product}',${convertPrizeSum.prize},${convertPrizeSum.countDrawnMode},${convertPrizeSum.countPatMode},${convertPrizeSum.sumPossessAmount},${convertPrizeSum.sumPrizeAmount})" +
          s" ON DUPLICATE KEY UPDATE count_drawn_mode=count_drawn_mode+${convertPrizeSum.countDrawnMode},count_pat_mode=count_pat_mode+${convertPrizeSum.countPatMode},sum_possess_amount=sum_possess_amount+${convertPrizeSum.sumPossessAmount},sum_prize_amount=sum_prize_amount+${convertPrizeSum.sumPrizeAmount}"
        SQL.apply(command).update().apply()
        convertPrizeSum.id
      }
    }
    def saveOrUpdateShopPrizeSum (shopPrizeSum: ShopPrizeSum)(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = shopPrizeSum.ecode
        val tableName = s"${tablePrefix}_shop_prize_sum"
        val command = s"INSERT INTO ${tableName}(id,date,activity_id,product_id,prize_id,shop_id,count_pat_mode)" +
          s" VALUES('${shopPrizeSum.id}',${shopPrizeSum.ecode},'${shopPrizeSum.date}',${shopPrizeSum.activity},'${shopPrizeSum.product}',${shopPrizeSum.prize},${shopPrizeSum.shop},${shopPrizeSum.count_pat_mode})" +
          s" ON DUPLICATE KEY UPDATE count_pat_mode =count_pat_mode+${shopPrizeSum.count_pat_mode}"

        SQL.apply(command).update().apply()
        shopPrizeSum.id
      }
    }
    def insertPrizeDetails (array: Seq[Any])(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val id = JDBCUtils.generateId
        val ecode: String = array(9).toString
        val tableName = s"${ecode}_exchange_prize_details"
        val commandcount = s"SELECT COUNT(*) as count  FROM ${tableName}"
        val count = (SQL.apply(commandcount).map(re => re.int("count")).single().apply())
        if (count.getOrElse(0) >= (999)) {
          val limit = count.getOrElse(0) - 999;
          val commanddel = s"DELETE FROM ${tableName}  ORDER BY create_time LIMIT ${limit}"
          SQL.apply(commanddel).execute().apply()
        }
        var command = s"INSERT INTO ${tableName}(id,prize_order,product_name,activity_name,prize_name,prize_status,user_name,open_id,is_first_get,create_time)" +
          s"VALUES('${id}','${array(0)}','${array(1)}','${array(2)}','${array(3)}','${array(4)}','${array(5)}','${array(6)}','${array(7)}','${array(8)}')"
        SQL.apply(command).update().apply()
        id
      }
    }
    /*对账*/
    def saveOrUpdateAccountsSum (accountsSum: AccountsSum)(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx { implicit session =>
        val tablePrefix = accountsSum.ecode
        val tableName = s"${tablePrefix}_accounts_sum"
        val command = s"INSERT INTO ${tableName}(id,date,produce_id,activity_id,sum_money,tidy_money,change_money)" +
          s" VALUES('${accountsSum.id}','${accountsSum.date}','${accountsSum.product_id}',${accountsSum.activity_id},${accountsSum.sum_money},${accountsSum.tidy_money},${accountsSum.change_money})" +
          s" ON DUPLICATE KEY UPDATE sum_money =sum_money+${accountsSum.sum_money},tidy_money =tidy_money+${accountsSum.tidy_money},change_money =change_money+${accountsSum.change_money}"
        SQL.apply(command).update().apply()
        accountsSum.id
      }
    }

    //
    def insertAccountsDetails (array: Seq[Any])(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx {
        implicit session =>
          val id = JDBCUtils.generateId
          val ecode: String = array.apply(array.size - 1).toString
          val tableName = s"${ecode}_accounts_details"
          var userName = array(4).toString
          if (StringUtils.isEmpty(userName)) userName = array(6).toString
          val isStatus = array.apply(array.size - 2).toString.toInt
          if (isStatus == 2) {
            val commandcount = s"SELECT COUNT(*) as count FROM ${tableName}"
            val count = (SQL.apply(commandcount).map(re => re.int("count")).single().apply())
            if (count.getOrElse(0) >= (999)) {
              val limit = count.getOrElse(0) - 999;
              val commanddel = s"DELETE FROM ${tableName}  ORDER BY TIME LIMIT ${limit}"
              SQL.apply(commanddel).execute().apply()
            }
            val command = s"INSERT INTO ${tableName}(id,produce_name,activity_name,time,openid,user_name,money)" +
              s"VALUES('${id}','${array(0)}','${array(1)}','${array(2)}','${array(3)}','${userName}',${array(5)})"
            SQL.apply(command).update().apply()
            id
          } else {
            s"金额统计无效数据"
          }
      }
    }
    def insertCountPrizeDetails (array: Seq[Any])(implicit ec: ExecutionContext): Future[String] = Future {
      DB localTx {
        implicit session =>
          val id = JDBCUtils.generateId
          val ecode: String = array.apply(array.size - 1).toString
          val tableName = s"${ecode}_count_prize_details"
          val isStatus = array.apply(3).toString.toInt
          val commandcount = s"SELECT COUNT(*) as count FROM ${tableName}"
          val count = (SQL.apply(commandcount).map(re => re.int("count")).single().apply())
          if (count.getOrElse(0) >= (999)) {
            val limit = count.getOrElse(0) - 999;
            val commanddel = s"DELETE FROM ${tableName}  ORDER BY TIME LIMIT ${limit}"
            SQL.apply(commanddel).execute().apply()
          }
          val command = s"INSERT INTO ${tableName}(id,_scan_code_order_id,_scan_time_prize_id,_user_id,_status,_use_scan_time,_scan_time_prize_type,_scan_time_prize_amount,_name,_tel,_address,_post_code,_create_time,_update_time,_duijiang_status)" +
            s"VALUES('${id}','${array(0)}','${array(1)}','${array(2)}','${array(3)}','${array(4)}',${array(5)},'${array(6)}','${array(7)}','${array(8)}','${array(9)}','${array(10)}','${array(11)}','${array(12)}','${array(13)}')"
          SQL.apply(command).update().apply()
          id

      }
    }
    def isDuplicateLog (key: String): Boolean = DB localTx { implicit session =>
      val value = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis())
      val tableName = s"_prize_filter"
      val commandcount = s"SELECT COUNT(*) as count  FROM ${tableName} where id='${key}'"
      val count = (SQL.apply(commandcount).map(re => re.int("count")).single().apply())
      if (count.getOrElse(0) == 1) {
        true
      } else {
        //if id non-exists then insert key
        var command = s"INSERT INTO ${tableName}(id,value)" +
          s"VALUES('${key}','${value}')"
        SQL.apply(command).update().apply()
        false
      }
    }


  }
