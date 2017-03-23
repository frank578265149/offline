package bigdata.analysis.scala.utils;


import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 配置管理组件
 * @author huisx
 */
public class ConfigurationManager {
	

	private static Properties prop = new Properties();
	private static Logger logger = Logger.getLogger(ConfigurationManager.class);
	/**
	 * 静态初始化
	 *
	 */
	static {
		try {
			InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
			String jsonString = sendGet("http://info.sys.v5q.cn/info_bireport");
			prop.load(in);
			JSONUtils.json2properties(prop,  new JSONObject(jsonString));
		} catch (Exception e) {
			logger.error("ConfigurationManager  error "+e);
			throw new RuntimeException("ConfigurationManager  error "+e);
		}

	}

	public static void main(String[] args) {
		String abc = getProperty("area_id");
	}
	/**
	 * 获取指定key对应的value
	 * 

	 * @param key
	 *
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			logger.error("ConfigurationManager getInteger error "+e);
			throw new RuntimeException("ConfigurationManager getInteger error "+e);
		}
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			logger.error("ConfigurationManager getBoolean error "+e);
			throw new RuntimeException("ConfigurationManager getBoolean error "+e);
		}
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			logger.error("ConfigurationManager getLong error "+e);
			throw new RuntimeException("ConfigurationManager getLong error "+e);
		}
	}
	public static String sendGet(String url){
		String result = "";
		BufferedReader in = null;
		try {
			String urlNameString = url;
			URL realUrl = new URL(urlNameString);
			// 打开和URL之间的连接
			URLConnection connection = realUrl.openConnection();
			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent",
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 建立实际的连接
			connection.connect();
			// 获取所有响应头字段
			Map<String, List<String>> map = connection.getHeaderFields();
			// 遍历所有的响应头字段
			// for (String key : map.keySet()) {
			//    System.out.println(key + "--->" + map.get(key));
			//}
			// 定义 BufferedReader输入流来读取URL的响应
			in = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("ConfigurationManager 发送GET请求出现异常！ error "+e);
			throw new RuntimeException("ConfigurationManager 发送GET请求出现异常 error "+e);
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return result;
	}


	
}
