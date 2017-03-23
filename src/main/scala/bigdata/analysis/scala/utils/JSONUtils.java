package bigdata.analysis.scala.utils;





import org.codehaus.jettison.json.JSONObject;


import java.util.Iterator;
import java.util.Properties;


/**
 * 处理json数据格式的工具类
 *
 * @version 1.0
 */
public class JSONUtils {
	public static Properties json2properties(Properties pro, JSONObject obj){
		Iterator iter = obj.keys();
		try {
			while (iter.hasNext()) {
				String key = iter.next().toString();
				pro.setProperty(key, obj.get(key).toString());
			}
		}catch (Exception e){
			e.getMessage();
		}
		return pro;
	}
}
