package bigdata.analysis.scala.dao;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.StreamingContext;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 服务器端（Server）多线程
 * 接收Byte数组
 * 该程序对应的客户端python脚本如下：
 *
 #!/usr/bin/env python
 # -*- coding:utf-8 -*-
 import socket
 '########定义常量'
 HOST='192.168.33.30'
 PORT=5678
 BUFSIZE=1024
 CONTEXT_ = "HELLO"
 '########Socket建立连接'
 sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
 sock.connect((HOST,PORT))
 print ("connected");
 #String--->byte[]
 sock.send(bytes(CONTEXT_, encoding = "utf8") )#发送byte数组
 print ("send data...");
 #byte[]---String
 returnData = sock.recv(BUFSIZE)
 print(str(returnData, encoding = "utf8"))
 '########关闭连接'
 sock.close()
 上边的代码保存为.py即可在IDE中运行
 但是如果上传到linux运行时会发现报错，需要将str函数的第二个参数utf-8编码去掉
 下边是linux运行报错信息：
 Traceback (most recent call last):
 File "ClientToServerScript.py", line 18, in <module>
 print(str(returnData, encoding = "utf8"))
 运行的时候直接：
 python xxxx.py即可运行
 *
 *
 *
 *
 *
 */
public class MultiClientByte extends Thread{
    private static Logger logger = Logger.getLogger(MultiClientByte.class);
    private Socket client;
    private int port;
    private static  StreamingContext ssc2;
    public MultiClientByte(Socket c) {
        this.client = c;
    }
    public MultiClientByte(StreamingContext ssc) {
        this.ssc2=ssc;
    }
    String context = null;
    public void run() {
        try {
            logger.info("服务端开启新的线程........"+Thread.currentThread().getName());
            DataInputStream in = new DataInputStream(client.getInputStream());
            PrintWriter out = new PrintWriter(client.getOutputStream());
            while (true) {
                try{
                    Thread.sleep(200);
                }catch (Exception ex){
                    logger.error("MultiClientByte Thread sleep:" + ex.getMessage());
                    ex.printStackTrace();
                }

                if(in.available()>0){//收到数据
                    //byte--->String
                    byte[]  b = new byte[in.available()];
                    for(int i = 0;i < b.length;i++){
                        b[i] = (byte)in.read();
                    }
                    context = new String(b,"utf-8");
                    logger.info("服务端接收到："+context);
                    //发送String
                    out.println("服务端已经接收到了您发送的："+context);
                    out.flush();
                    logger.info(MultiClientByte.ssc2);
                }
                if(context!=null && context.equals("end")){
                    try{
                        logger.info("222222222222222222222222222222222start stop ssc ：");
                        MultiClientByte.ssc2.stop(true,true);
                        logger.info("streaming stop ssc ：");
                        break;
                    }catch (Exception e){
                        logger.error("MultiClientByte ssc error......." + e);
                    }
                }
            }
            //while循环结束之后
            client.close();
            System.exit(0);
        } catch (IOException ex) {
            logger.error("MultiClientByte 服务端异常.......");
        } finally {

        }
    }

    public  void StartEngine(int port) throws IOException {
        ServerSocket server = new ServerSocket(port);
        while (true) {
            MultiClientByte mc = new MultiClientByte(server.accept());
            mc.start();
        }
    }
}
