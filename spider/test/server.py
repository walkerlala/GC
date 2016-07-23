#!/usr/bin/python3
#coding:utf-8

import socket
import sys
import threading
import os

TCP_IP = '127.0.0.1'
TCP_PORT = 5005
#BUFFER_SIZE should be power of 2 to speed up(hardware reason)
BUFFER_SIZE = 512

sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.bind((TCP_IP,TCP_PORT))
#listen up to 5 client(what if that exceed?)
sock.listen(5)

server_log_name = "server_log.log"
server_log = open(server_log_name,"w+")

def handle_connection(conn,file_handle):
    while(True):
        try:
            data = conn.recv(BUFFER_SIZE)
            if data:
                file_handle.write(data.decode("utf-8"))
                file_handle.flush()
            else:
                file_handle.close()
                conn.close()
                break
        except Exception as e:
            server_log.write("Error: %s\n" % str(e))
            print ("Error: ", str(e), file=sys.stderr)
            continue

# dir store all the links file
dir = "dir"
if not os.path.exists(dir):
    os.mkdir(dir)

#The connection is actually another socket assigned by the kernel
#we are not handling the situation where there are too many connection requests
thread_list = []
counter = 0
print("Server start running now", end="\n")
while True:
    # 10 thread at a time
    if len(thread_list) > 10:
        for thread in thread_list:
            thread.join()
        thread_list = []
    conn, addr = sock.accept()
    server_log.write("connection address: %s\n" % str(addr))
    try:
        file_handle = open(dir + "/" + "server_recv." + str(counter),"w+")
        counter = counter + 1
        t = threading.Thread(target=handle_connection,args=(conn,file_handle));
        thread_list.append(t)
        t.start()
    except Exception as e:
        server_log.write("Error: " + str(e) + "\n")

#remove empty log file
if (os.path.getsize(server_log_name) == 0):
    os.remove(server_log_name)


