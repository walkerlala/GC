#!/usr/bin/python3 -u
#encoding:utf-8

# pylint: disable=superfluous-parens,invalid-name

""" master node """

import socket
import datetime
import pickle
import threading
import random
import Bloom_filter
#from unbuffered_output import uopen

#this would cause server timeout
#socket.setdefaulttimeout(60)

#the initial links to crawl
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
            as we have not implemented the prio method """
        with open(self.links_file, "r") as f:
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

    def append(self, link):
        """ feed links in to the internal prio queue,
            note that those links are ordered by prio """
        index = int(self.randobj.random() * len(self.links))
        self.links.insert(index, link)

class Manager:
    """Core Manager """

    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        self.ip = ip
        self.port = port
        self.buff_size = buff_size
        self.listen_num = listen_num

        #socket initialization
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.listen(self.listen_num)

        #bloom_filter里面存放已经爬过的链接(或许没有爬成功)
        self.bloom_filter = Bloom_filter.Bloom_filter(10000,
                                         0.001,  #error rate
                                         filename=("/tmp/blmftr_tmp",-1),
                                         start_fresh=True)
        self.bf_lock = threading.Lock() #lock to access bloom_filter

        self.pr_lock = threading.Lock() #lock to access priority queue
        self.prio_que = PriQueue()          #Manager's priority queue
        self.prio_que.get_links_from_disk() #initially get links from disk
        self.prio_ful_threshold = 10000000 # 100bytes/links x 5000000 ~ 1G

        self.thread_list = []        #list of threads in Manager
        self.thread_num = thread_num #how many thread we should start

        self._log = open("Manager.log", "w+") #log file
        self.log_lock = threading.Lock()       #lock to access log file

        #record all the links we have crawled
        self._links_track = open("how-many-links.links", "w+")

        self._links_lock = threading.Lock()   #lock
        self._nsent = 100 #how many links we send to crawler per request

        #MACRO,represent whether crawler want to send back links or get links from here
        self.SEND = 1
        self.REQUEST = 0

        self.total_links_threshold_nr = 100000
        self.total_links_crawled_nr=0

    def handle_connection(self, conn):
        """ handle connection with some crawler """

        #set timeout for this connection, so failure of one crawler would
        #not waste resource of the Manager
        conn.settimeout(60)

        method = None
        data_buf = []
        try:
            method = self.get_conn_type(conn)
            if (method == self.SEND):#client want to send links to Manager
                conn.sendall(b'OK')
                while True:
                    data = conn.recv(self.buff_size)
                    if data:
                        data_buf.append(data)
                    else:
                        break
                #result dict is a dict: { link: {set of links or 'FAIL'}
                data = b''.join(data_buf)
                _result_dict = pickle.loads(data)
                crawled_links = []
                for key, value in _result_dict.items():
                    #没爬成功的，就是'FAIL'(我们对一个链接只爬一次，无论成功与否)
                    if (value == 'FAIL'):
                        with self.bf_lock:
                            #加入bloom_filter之中，否则这个链接会被重爬
                            self.bloom_filter.add(key)
                    else:
                        with self.bf_lock:
                            crawled_links.append(key)
                            self.bloom_filter.add(key)
                            # limit links count
                            if len(self.prio_que) < self.prio_ful_threshold:
                                for sub_link in value:
                                    if sub_link not in self.bloom_filter:
                                        with self.pr_lock:
                                            self.prio_que.append(sub_link)
                #write all the link to `self._links_track`
                with self._links_lock:
                    self.total_links_crawled_nr = self.total_links_crawled_nr + len(crawled_links)
                    for link in crawled_links:
                        self._links_track.write(str(link) + "\n")
                    if self.total_links_crawled_nr > self.total_links_threshold_nr:
                        self.prio_que = []
            elif (method == self.REQUEST):
                conn.sendall(b'OK')
                """ 假如prioQueue里面没有了就返回一个空的lists """
                data = None
                links_buffer = []
                with self.pr_lock:
                    nsent = self._nsent if self._nsent <= len(self.prio_que) else len(self.prio_que)
                    try: #需要捕捉异常。万一内存爆了然后self.prio_que放不下东西了呢？
                        for _ in range(nsent):  #一次发送多少条链接？
                            links_buffer.append(self.prio_que.get())
                    except Exception as e:
                        raise Exception("Exception:[%s] geting links from PriQueue" % str(e))
                #如果prio_que里面没有链接了，我们发送过去的就是一个空的list了
                data = pickle.dumps(links_buffer)
                try:
                    #print("Sending data...\n")
                    conn.sendall(data)
                    #print("Finish sending data.\n")
                except Exception as e:
                    raise
            else:
                raise Exception("UNKNOWN CONNECTION TYPE")
        except Exception as e:
            #不能再向上抛异常了，因为这是多线程模型。
            #异常应该在本函数内处理
            with self.log_lock:
                t = str(datetime.datetime.now())
                self._log.write("[%s]::Exception:[%s]\n" % (t,str(e)))
            print("Fatal exception[%s] in Manager.handle_connection()\n" % str(e))
        finally:
            #print("One thread close connection\n")
            conn.close()

    def get_conn_type(self, conn):
        """ get connection type(SEND or REQUEST) of this connection """
        try:
            data = conn.recv(self.buff_size)
            print("data recv in get_conn_type:[%s]" % str(data))
            if (data == b'SEND'):
                return self.SEND
            elif (data == b'REQUEST'):
                return self.REQUEST
            else:
                return None
        except Exception:
            raise

    def run(self):
        """ start Manager """
        self._log.write("Manager start running at: [%s]\n" %
                              str(datetime.datetime.now()))
        while(True):
            only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            #print("Connection established: %s\n" % str(addr))
            self._log.write("Connection established: %s\n" % str(addr))
            t = threading.Thread(target=self.handle_connection, args=(conn,))
            #t.daemon = True
            self.thread_list.append(t)
            t.start()

if __name__ == "__main__":
    manager = Manager("0.0.0.0", 5005)
    manager.run()

