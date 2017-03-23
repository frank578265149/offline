#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import socket
if(len(sys.argv)!=4):
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print('         please input 3 params: ip + port + context          ')
        print('             for example:                                    ')
        print('                     python xxx.py 127.0.0.1 1234 end        ')
        print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        sys.exit(0)

HOST=sys.argv[1]
#HOST = input('please input ip:')

PORT=sys.argv[2]
#PORT = input('please input port:')
BUFSIZE=1024
CONTEXT_str= sys.argv[3]
#CONTEXT_str = input('please input context:')
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST,int(PORT)))
print ("connected");
#String--->byte[]
sock.send(bytes(CONTEXT_str) )
print ("send data...");
#byte[]---String
returnData = sock.recv(BUFSIZE)
print(str(returnData))
sock.close()
