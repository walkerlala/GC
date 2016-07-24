#!/usr/bin/python3
#encoding:utf-8

import socket
import os
import datetime
import sys
import pickle
import threading
import Bloom_filter
import random

#this would cause server timeout
#socket.setdefaulttimeout(60)

links_file = "Manager.links"

class PriQueue:
    """ a priority queue that used to stored links
        and dispatch links to the right """
    def __init__(self):
        self.buf = []
        self.links = []
        self.links_file = links_file
        self.randobj = random.Random(5) #use to generate random position for inserting


    def get_links_from_disk(self):
        """ This is test method. Use this to get links
            from disk, as we have not implement the 
            prio method """
        with open(self.links_file,"r") as f:
            for line in f:
                self.links.append(line.strip())
            self.links = list(set(self.links)) #hopefully that the list would get randomized

    def get(self):
        """ get links according to prio of links """
        if len(self.links):
            link = self.links.pop()
            return link
        else:
            raise Exception("No links in PriQueue")

    def append(self,link):
        """ feed links in to the internal prio queue,
            note that those links are ordered by prio """
        index = self.randobj.random() * len(self.links)
        self.links.insert(index,link)
    
class Manager:
    """Core Manager """
    
    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        self.ip = ip
        self.port = port
        self.buff_size = buff_size
        self.listen_num = listen_num
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(self.listen_num)
        #bloom_filter里面存放已经爬过的链接(或许没有爬成功)
        self.bloom_filter = Bloom_filter.Bloom_filter(100000, 0.01, filename="/tmp/blmftr_tmp",
                                                      start_fresh=True)
        self._lock = threading.Lock() #lock to access bloom_filter
        self.prio_que = PriQueue()
        self.prio_que.get_links_from_disk()
        self.thread_list = []
        self.thread_num = thread_num
        self._log = open("Manager.log","w+")
        self.SEND = 1
        self.REQUEST = 0

    def handle_connection(self, conn):
        method = None
        data_buf = []
        try:
            method = self.get_conn_type(conn)
            if (method == self.SEND):#client want to send links to Manager
                while True:
                    data = conn.recv(self.buff_size)
                    if data:
                        data_buf.append(data)
                    else:
                        break
                #result dict is a dict: { link: {set of links or 'FAIL'}
                data = b''.join(data_buf) 
                _result_dict = pickle.loads(data)
                for key, value in _result_dict.items():
                    #if fail to crawl the original link, then just discard it
                    if (value == 'FAIL'):
                        with self._lock:
                            #加入bloom_filter之中，否则这个链接会被重爬
                            self.bloom_filter.add(key) 
                    else:
                        with self._lock:
                            self.bloom_filter.add(key)
                            for sub_link in value:
                                if sub_link not in self.bloom_filter:
                                    self.prio_que.append(sub_link)
            elif (method == self.REQUEST):
                data = None
                try:
                    links_buffer=[]
                    for _ in range(20):
                        links_buffer.append(self.prio_que.get())
                    data = pickle.dumps(links_buffer)
                except Exception as e:
                    raise Exception("Exception happen when trying to get links from PriQueue")
                try:
                    conn.sendall(data)
                except Exception as e:
                    raise
            else:
                raise Exception("UNKNOWN CONNECTION TYPE")
        except Exception as e:
            #不能再向上抛异常了，因为这是多线程模型。
            #异常应该在本函数内处理
            with self._lock:
                self._log.write("Exception:[%s]\n" % str(e))
            print("Exception[%s] in Manager.handle_connection()\n" % str(e))
                
    def get_conn_type(self, conn):
        try:
            data = conn.recv(self.buff_size)
            if (data == b'SEND'):
                return self.SEND
            elif (data == b'REQUEST'):
                return self.REQUEST
            else:
                return None
        except Exception as e:
            raise

    def run(self):
        self._log.write("Manager start running at: %s" %
                              datetime.datetime.now().strftime("%B %d, %Y"))
        while(True):
            #only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            self._log.write("Connection established: %s\n" % str(addr))
            t = threading.Thread(target=self.handle_connection, args=(conn,))
            self.thread_list.append(t)
            t.start()

if __name__ == "__main__":
    manager = Manager("127.0.0.1",5005)
    manager.run()

