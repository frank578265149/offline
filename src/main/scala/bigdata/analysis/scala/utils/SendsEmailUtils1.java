package bigdata.analysis.scala.utils;

import org.apache.log4j.Logger;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Created by frank on 17-2-27.
 */
public class SendsEmailUtils1 {
    private static Logger logger = Logger.getLogger(SendsEmailUtils1.class);
    //tosend指定收信人
    public static  void send(String title,String context, String tosend){
        Properties props = new Properties();
        // 开启debug调试
        props.setProperty("mail.debug", "true");
        // 发送服务器需要身份验证
        props.setProperty("mail.smtp.auth", "true");
        // 设置邮件服务器主机名
        props.setProperty("mail.host", "mail.sao.so");
        // 发送邮件协议名称
        props.setProperty("mail.transport.protocol", "smtp");
        // 设置环境信息
        Session session = Session.getInstance(props);

        // 创建邮件对象
        Message msg = new MimeMessage(session);
        try{
            msg.setSubject(title);
            // 设置邮件内容
            msg.setText(context);
            // 设置发件人
            msg.setFrom(new InternetAddress("lilu@sao.so"));

            Transport transport = session.getTransport();
            // 连接邮件服务器
            transport.connect("lilu", "xMHT123456");
            // 发送邮件
            transport.sendMessage(msg, new Address[] {new InternetAddress(tosend)});
            // 关闭连接
            transport.close();
            logger.info("lilu@sao.so send mail complete!");
        }catch (Exception e){
            logger.error("send mail fails:" + e.getMessage());
            e.printStackTrace();
        }
    }
    //发给自己
    public static  void send(String title,String context){
        Properties props = new Properties();
        // 开启debug调试
        props.setProperty("mail.debug", "true");
        // 发送服务器需要身份验证
        props.setProperty("mail.smtp.auth", "true");
        // 设置邮件服务器主机名
        props.setProperty("mail.host", "mail.sao.so");
        // 发送邮件协议名称
        props.setProperty("mail.transport.protocol", "smtp");
        // 设置环境信息
        Session session = Session.getInstance(props);

        // 创建邮件对象
        Message msg = new MimeMessage(session);
        try{
            msg.setSubject(ConfigurationManager.getProperty("redis.sentinel") + title);
            // 设置邮件内容
            msg.setText(context);
            // 设置发件人
            msg.setFrom(new InternetAddress("lilu@sao.so"));

            Transport transport = session.getTransport();
            // 连接邮件服务器
            transport.connect("lilu", "xMHT123456");
            // 发送邮件
            transport.sendMessage(msg, new Address[] {new InternetAddress("lilu@sao.so")});
            // 关闭连接
            transport.close();
            logger.info("lilu@sao.so send mail complete!");
        }catch (Exception e){
            logger.error("send mail fails:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("11111111111");
        SendsEmailUtils1.send("JavaMail sao  测试", "这是一封由JavaMail发送的邮件！","lilu@sao.so");
        Properties props = new Properties();


    }
}
