/*
package bigdata.analysis.java.hbase;

import bigdata.analysis.java.dao.ErrInsertHbase;
import bigdata.analysis.java.hbase.entity.People;


import java.util.HashMap;
import java.util.Map;

*/
/**
 * Created by shengjk1 on 2016/9/28.
 *//*

public class TestHbase {

	*/
/**
	 * 测试InsertHbase
	 * @param args
	 *//*

	public static void main(String[] args) {
		People people=new People(null,12);
		People people2=new People("a",121);
		People people3=new People("b",122);
		People people4=new People("v",123);

		Map<String,Object> map=new HashMap();
		map.put("a11",people);
		map.put("a22",people2);
		map.put("a33",people3);
		map.put("a44",people4);

		try {
//			ObjInsertHbase.insert(map,"test");
			System.out.println("over !");
			ErrInsertHbase.insert("a0","aa","test");
//			HbaseCreate.create("test1");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
*/
