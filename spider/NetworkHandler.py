#!/usr/bin/python
#coding:utf-8

import socket
import threading
import sys
import os
import time
import pickle

socket.setdefaulttimeout(60)

class NetworkHandler:
    """ handle network """

    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
       
    #data have to be raw(byte) string, not unicode 
    def send(self, data):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._ip, self._port))
            #与worker建立连接，表示要发送数据
            if (self.establish_context(sock, b"SEND")):
                sock.sendall(data)
            else:
                raise Exception("Cannot establish valid connection with worker/server")
            sock.close()
        except Exception as e:
            raise
    def request(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._ip, self._port))
            if (self.establish_context(sock, b"REQUEST")):
                links_raw = []
                while (True):
                    try:
                        data = sock.recv(512)
                        print("NetworkHandler get some data from Manager:[%s]\n" % str(data))
                        if not data:  #finish getting data
                            break
                        links_raw.append(data)
                    except Exception as e:
                        #既然中断了连接，数据肯定不完整
                        raise 
                links = b''.join(links_raw)
                print("NetworkHandler get data:[%s]\n" % str(links))
                sock.close()
                return pickle.loads(links)
            else:
                raise Exception("Cannot establish valid connection with worker/server")
        except Exception as e:
            raise

    def establish_context(self,sock,connect_type):
        try:
            sock.sendall(connect_type)
        except Exception as e:
            return False
        return True
     ###       not ready for this handshake yet
        #try:
        #    received = sock.recv(16)
        #    return True if received and (received.upper() == b"OK") else False
        #except Exception as e:
        #    return False 
