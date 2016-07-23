#!/usr/bin/python3
#coding:utf-8

import socket
import threading
import sys
import os
import time

#TCP_IP = '119.29.166.19'
#TCP_IP = '116.56.143.101'
TCP_IP = '127.0.0.1'
TCP_PORT = 5005

socket.setdefaulttimeout(30)

client_log_name = "client_log.log"
client_log = open(client_log_name,"w+")

def send_to_server(links):
    try:
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((TCP_IP,TCP_PORT))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        for link in links:
            try:
                sock.sendall(link.encode('utf-8'))
            except Exception as e:
                client_log.write("Error: %s: %s\n" % (str(e),str(link)))
                continue
            client_log.write("Successfully write one link: %s\n" % str(link))
            #time.sleep(1)
        sock.close()
    except Exception as e:
        client_log.write("Error: " + str(e) + "\n")

print("Server start running now", end="\n")
thread_list = []
with open("links.links","r") as f:
    tmp_list = []
    for count, link in enumerate(f):
        if(len(thread_list) >= 10):
            for thread in thread_list:
                thread.join()
            thread_list = []
        if count % 100 != 0:
            tmp_list.append(link)
        else:
            t = threading.Thread(target=send_to_server,args=(tmp_list,))
            thread_list.append(t)
            tmp_list=[]
            t.start()

#remove empty client_log
if (os.path.getsize(client_log_name) == 0):
    os.remove(client_log_name)

