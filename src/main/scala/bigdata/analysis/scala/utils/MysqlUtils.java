package bigdata.analysis.scala.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by gaochao on 2017/1/6.
 */
public class MysqlUtils {
    public static void insertOrUpdate(String line){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String date_ = sdf.format(new Date());
        String[] keys = line.split("\\|");
        String enterprise = keys[0];
        String activityId = keys[1];
        String prizeId = keys[2];
        String flowCode = keys[3];
        String pageCode = keys[4];
        String month = keys[5];
        String count = keys[6];
        String lost = keys[7];
        String id = enterprise+"|"+activityId+"|"+prizeId+"|"+flowCode+"|"+pageCode+"|"+month;
        String sqls = "insert into "+ enterprise+"_page_lost(`id`,`date`,`activity_id`,`flow_code`,`prize_id`,`page_code`,`count`,`lost`) values ( '"+id+"','"+date_+"','"+activityId+"','"+flowCode+"','"+prizeId+"','"+pageCode+"',"+count+","+lost+")"
                + " ON DUPLICATE KEY UPDATE `count` ="+ count+" , `lost` = "+lost;
        System.out.println(sqls);
        Connection conn = null;
        Statement stmt = null;
        try {
            conn= C3P0Util.getConnection();
            stmt = conn.createStatement();
            int rs = stmt.executeUpdate(sqls);

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            C3P0Util.close(conn,stmt);
        }


    }
}
