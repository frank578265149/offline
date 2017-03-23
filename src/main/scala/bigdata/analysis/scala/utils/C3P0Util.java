package bigdata.analysis.scala.utils;

/**
 * Created by gaochao on 2017/1/6.
 * 获取jdbc的连接
 */

import bigdata.analysis.scala.utils.ConfigurationManager;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class C3P0Util {
    static ComboPooledDataSource dataSource;
    static{
        String url = ConfigurationManager.getProperty("jdbc.url");
        String user = ConfigurationManager.getProperty("jdbc.user");
        String password = ConfigurationManager.getProperty("jdbc.password");
        String driver = ConfigurationManager.getProperty("jdbc.driver");
        try {
            dataSource=new ComboPooledDataSource();
            dataSource.setUser(user);
            dataSource.setPassword(password);
            dataSource.setJdbcUrl(url);
            dataSource.setDriverClass(driver);
            dataSource.setInitialPoolSize(11);
            dataSource.setMinPoolSize(10);
            dataSource.setMaxPoolSize(100);
            dataSource.setAcquireIncrement(5);
            dataSource.setMaxStatements(50);
            dataSource.setMaxIdleTime(60);
        }catch (PropertyVetoException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        }catch(SQLException e){
            throw new RuntimeException( "无法从数据源获取连接 ",e);
        }
    }
    /**
     * 关闭连接
     */
    public static void close(Connection conn, Statement pst){

        if(pst!=null){
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
