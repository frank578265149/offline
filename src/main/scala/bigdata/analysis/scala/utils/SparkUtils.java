package bigdata.analysis.scala.utils;



import bigdata.analysis.scala.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark工具类
 * @author Huisx
 *
 */
public class SparkUtils {
	
	/**
	 * 根据当前是否本地测试的配置
	 * 决定，如何设置SparkConf的master
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			conf.setMaster("local[1]");
		}  
	}
	
	/**
	 * 获取SQLContext
	 * 根据当前,是本地使用SQLContext；否则，创建HiveContext
	 * @param sc
	 * @return SQLContext
	 */
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new SQLContext(sc);
		}
	}
	

	
}
