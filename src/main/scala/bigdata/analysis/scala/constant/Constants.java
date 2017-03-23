package bigdata.analysis.scala.constant;

import scala.Serializable;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants  extends Serializable{

	/**
	 * JDBC
	 */
	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";

	/**
	 * Kafka
	 */
	String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
	String KAFKA_TOPICS01 = "kafka.topics01";
	String KAFKA_TOPICS02 = "kafka.topics02";
	String KAFKA_TOPICS03 = "kafka.topics03";

	/**
	 * Spark
	 */
	String SPARK_APP_NAME = "group-owner-prize";
	String SPARK_LOCAL = "spark.local";

	
}
