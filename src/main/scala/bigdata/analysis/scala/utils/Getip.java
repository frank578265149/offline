package bigdata.analysis.scala.utils;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by Sigmatrix on 2016/10/19.
 */
public class Getip {
    public static String getContent(String ip) {
        String result = "";
        BufferedReader br = null;
        String data = "";
        String province = "";
        String city ="";
        String district = "";
        String provincename="";
        String cityname ="";
        String districtname = "";
        Integer area_id = 0;
        data =  province + "#" + city + "#" +  district + "#" + provincename+ "#" + cityname +"#"+ districtname+"#"+ip;
        try {
            URL url = new URL(ConfigurationManager.getProperty("stream.ipServer") + ip);
            URLConnection connection = url.openConnection();
            connection.connect();
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line = "";
            while ((line = br.readLine()) != null) {
                result += line;
            }
            JSONObject json = new JSONObject(result);
            if(! json.isNull("provinceId"))   province = json.getString("provinceId");
            if(! json.isNull("cityId"))   city = json.getString("cityId");
            if(! json.isNull("districtId"))  district = json.getString("districtId");
            if(! json.isNull("province"))   provincename = json.getString("province");
            if(! json.isNull("city"))   cityname = json.getString("city");
            if(! json.isNull("district"))  districtname = json.getString("district");
            data = province + "#" + city + "#" +  district + "#" + provincename + "#" + cityname + "#" +  districtname + "#" + ip;

        }
        catch (IOException e) {
            //SendEmailUtils.send("userError GetIp", "userError  GetIp 失败===========   "+e);
            e.printStackTrace();
        }
        catch (JSONException e) {
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }
        catch (ArrayIndexOutOfBoundsException e){
            System.out.println("ArrayIndexOutOfBoundsException:" + e.getMessage());
            e.printStackTrace();
        }catch (Exception e){
            System.out.println("Getip other Exception:" + e.getMessage());
        }


        finally {
            try {
                br.close(); }
            catch (IOException e) {
                e.printStackTrace();
            }
            catch (Exception e){
                System.out.println("Getip br open file Exception:" + e.getMessage());
            }
            return data;
        }
    }
}
