/*
package bigdata.analysis.java.hive;

*/
/**
 * Created by huisx on 2016/9/28.
 *//*

import java.sql.Connection;

import java.sql.DriverManager;

import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.Statement;



public class ConnHive {

    public static void main(String[] args) throws Exception{
        String querySQL = "SELECT a.name, a.id, a.sex FROM com58 a where a.id > 100 and a.id < 110 and sex='male'";
        hive2Txt(querySQL);

    }



    private static void hive2Txt(String querySQL)
            throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        // String dropSQL = "drop table com58";
        // 1 male 29 3d649ecc 3d649ecc@qq.com 20110304 20110402

        // 1.id 2.sex 3.age 4.name 5.mail 6.sDate 7.eDate

        //String createSQL = "create table aaqqa (id int, sex string, age int, name string, mail string, sDate bigint, eDate bigint) row format delimited fields terminated by ' '";
        String createSQL ="select * from abc ";
        // hive插入数据支持两种方式一种：load文件，令一种是 CTAS（create table as select...

        // 从另一个表中查询进行插入）

        // hive是不支持insert into...values(....)这种操作的

        // String insterSQL =

        // "LOAD DATA LOCAL INPATH '/home/june/hadoop/hadoop-0.20.203.0/tmp/1.txt' OVERWRITE INTO TABLE com58";

        Connection con = DriverManager.getConnection("jdbc:hive2://114.115.213.107:9103/default", "", "");
        Statement stmt = con.createStatement();
        // stmt.executeQuery(dropSQL); // 执行删除语句
        ResultSet res = stmt.executeQuery(createSQL); // 执行建表语句
        System.out.print(res);
        stmt.close();
        // stmt.executeQuery(insterSQL); // 执行插入语句

        //  ResultSet res = stmt.executeQuery(querySQL); // 执行查询语句
//      while (res.next()) {
//          System.out.println("name:\t" + res.getString(1) + "\tid:\t"  + res.getString(2) + "\tsex:\t" + res.getString(3));
//      }

    }

}
*/
