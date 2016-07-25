#!/usr/bin/python3
#encoding:utf-8

import socket
import datetime
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
        self.links = []
        self.links_file = links_file
        self.randobj = random.Random(5) #use to generate random position for inserting

    def __len__(self):
        return len(self.links)

    def get_links_from_disk(self):
        """ This is test method. Use this to get links from disk,
            as we have not implement the prio method """
        with open(self.links_file,"r") as f:
            for line in f:
                self.links.append(line.strip())
            self.links = list(set(self.links)) #hopefully that the list would get randomized
        print("PriQueue successfully get links from disk\n")

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
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self.sock.listen(self.listen_num)
        #bloom_filter里面存放已经爬过的链接(或许没有爬成功)
        self.bloom_filter = Bloom_filter.Bloom_filter(100000,
                                                 0.01,  #error rate
                                                 filename="/tmp/blmftr_tmp",
                                                 start_fresh=True)
        self.bf_lock = threading.Lock() #lock to access bloom_filter
        self.pr_lock = threading.Lock() #lock to access priority queue
        self.prio_que = PriQueue()
        self.prio_que.get_links_from_disk()
        self.thread_list = []
        self.thread_num = thread_num
        self._log = open("Manager.log","w+")
        self.log_lock = threading.Lock()
        #MACRO,represent whether crawler want to send back links or get links from here
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
                        with self.bf_lock:
                            #加入bloom_filter之中，否则这个链接会被重爬
                            self.bloom_filter.add(key) 
                    else:
                        with self.bf_lock:
                            self.bloom_filter.add(key)
                            for sub_link in value:
                                if sub_link not in self.bloom_filter:
                                    with self.pr_lock:
                                        self.prio_que.append(sub_link)
            elif (method == self.REQUEST):
                """假如prioQueue里面没有了就返回一个空的lists"""
                data = None
                links_buffer=[]
                if(len(self.prio_que)):
                    try:
                        with self.pr_lock:
                            for _ in range(5):  #一次发送多少条链接？
                                links_buffer.append(self.prio_que.get())
                    except Exception as e:
                        raise Exception("Exception happen when trying to get links from PriQueue")
                data = pickle.dumps(links_buffer)
                try:
                    print("Sending data...")
                    conn.sendall(data)
                    print("Finish sending data.\n")
                except Exception as e:
                    raise
            else:
                raise Exception("UNKNOWN CONNECTION TYPE")
        except Exception as e:
            #不能再向上抛异常了，因为这是多线程模型。
            #异常应该在本函数内处理
            with self.log_lock:
                self._log.write("Exception:[%s]\n" % str(e))
                self._log.flush()
            print("Exception[%s] in Manager.handle_connection()\n" % str(e))
        finally:
            print("close connection")
            conn.close()
                
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
        self._log.write("Manager start running at: %s\n" %
                              datetime.datetime.now().strftime("%B %d, %Y"))
        while(True):
            #only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            self._log.write("Connection established: %s\n" % str(addr))
            self._log.flush()
            t = threading.Thread(target=self.handle_connection, args=(conn,))
            self.thread_list.append(t)
            t.start()

if __name__ == "__main__":
    manager = Manager("127.0.0.1",5005)
    manager.run()

