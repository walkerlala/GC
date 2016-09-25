#!/usr/bin/python3 -u
#encoding:utf-8

# pylint: disable=superfluous-parens,invalid-name,broad-except

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

root_path = "../"
log_path = "../log/"

#the initial links to crawl
links_file = root_path + "manager.links"
how_many_links_file = root_path + "how-many-links.links"

class EmptyPriQueue(Exception):
    def __init__(self, *args):
        super(EmptyPriQueue, self).__init__(*args)

class PriQueue:
    """ a priority queue that used to stored links
        and dispatch links to the right."""

    def __init__(self):
        self.links = []
        self.links_by_addr = []        # every crawler use a prio queue
        self.queue_pos = {}             # to record which crawler use which prio queue
        self.links_file = links_file
        self.randobj = random.Random(7) #use to generate random position for inserting
        self.pr_lock = threading.Lock() #lock to access priority queue

    def __len__(self):
        with self.pr_lock:
            total_len = len(self.links)
            for lst in self.links_by_addr:
                total_len = total_len + len(lst)
            return total_len

    def get_links_from_disk(self):
        """ This is test method. Use this to get links from disk,
            as we have not implemented the prio method """
        with open(self.links_file, "r") as f:
            for line in f:
                self.links.append(line.strip())
            self.links = list(set(self.links)) #hopefully that the list would get randomized
        print("PriQueue successfully get links from disk\n")

    def get(self):
        """ get links randomly """
        if len(self.links):
            with self.pr_lock:
                return self.links.pop()
        else:
            raise EmptyPriQueue("Empty links\n")

    def append(self, link):
        """ feed links into the internal prio queue,
            note that those links are now NOT ordered by prio """
        index = int(self.randobj.random() * len(self.links))
        with self.pr_lock:
            self.links.insert(index, link)

    def get_by_addr(self, addr):
        """ get links from specific prio queue """
        pos = self.get_pos(addr)
        if len(self.links_by_addr) < pos + 1:
            return self.get()
        else:
            if len(self.links_by_addr[pos]):
                with self.pr_lock:
                    return self.links_by_addr[pos].pop()
            else:
                raise EmptyPriQueue("Empty links in pos:[%d]\n" % pos)

    def append_by_addr(self, link, addr):
        """ feed links into a specific prio queue according to conn """
        pos = self.get_pos(addr)
        if len(self.links_by_addr) < pos + 1:
            with self.pr_lock:
                self.links_by_addr.append([link])
        else:
            index = int(self.randobj.random() * len(self.links_by_addr[pos]))
            with self.pr_lock:
                self.links_by_addr[pos].insert(index, link)

    def get_pos(self, addr):
        """ get the index in self.links_by_addr by addr(every crawler have
            its prio queue"""
        if addr[0] in list(self.queue_pos.keys()):
            return self.queue_pos[addr[0]]
        else:
            self.queue_pos.update({addr[0]:len(self.queue_pos)})
            return len(self.queue_pos) -1

class Manager:
    """Core manager """

    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        """ Initialization """

        # FIXME should use a global conf file

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
                                         filename=("/tmp/blmftr_tmp", -1),
                                         start_fresh=True)
        self.bf_lock = threading.Lock() #lock to access bloom_filter

        self.prio_que = PriQueue()          #manager's priority queue
        self.prio_que.get_links_from_disk() #initially get links from disk
        self.prio_ful_threshold = 10000000 # 100bytes/links x 5000000 ~ 1G

        self.thread_list = []        #list of threads in manager
        self.thread_num = thread_num #how many thread we should start

        self._log = open(log_path + "manager.log", "w+") #log file
        self.log_lock = threading.Lock()       #lock to access log file

        #record all the links we have crawled
        self._links_track = open(how_many_links_file, "w+")

        self._links_track_lock = threading.Lock()   #lock
        self._nsent = 50 #how many links we send to crawler per request

        #MACRO,represent whether crawler want to send back links
        #or get links from here
        self.SEND = 1
        self.REQUEST = 0

    def handle_connection(self, conn, addr):
        """ handle connection with some crawler """

        #set timeout for this connection, so failure of one crawler would
        #not waste resource of the manager
        conn.settimeout(60)

        method = None
        data_buf = []
        try:
            method = self.get_conn_type(conn)
            if (method == self.SEND):#client want to send links to manager
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
                                        self.prio_que.append_by_addr(sub_link, addr)
                #write all the link to `self._links_track`
                with self._links_track_lock:
                    for link in crawled_links:
                        self._links_track.write(str(link) + "\n")

            elif (method == self.REQUEST):#client request some links
                conn.sendall(b'OK')
                """ 假如prioQueue里面没有了就会返回一个空的lists """
                data = None
                links_buffer = []
                try:
                    for _ in range(self._nsent):  #一次发送多少self._nsent条链接
                        links_buffer.append(self.prio_que.get_by_addr(addr))
                except EmptyPriQueue:
                    pass
                except Exception as e:
                    raise Exception("Exception:[%s] getting links from PriQueue" % str(e))
                #如果prio_que里面没有链接了，我们发送过去的就是一个空的list了
                data = pickle.dumps(links_buffer)
                try:
                    conn.sendall(data)
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
        finally:
            conn.close()

    def get_conn_type(self, conn):
        """ get connection type(SEND or REQUEST) of this connection """
        try:
            data = conn.recv(self.buff_size)
            if (data == b'SEND'):
                return self.SEND
            elif (data == b'REQUEST'):
                return self.REQUEST
            else:
                return None
        except Exception:
            raise

    def run(self):
        """ start manager """
        self._log.write("manager start running at: [%s]\n" % str(datetime.datetime.now()))
        while(True):
            #only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            self._log.write("Connection established: %s\n" % str(addr))
            t = threading.Thread(target=self.handle_connection, args=(conn, addr))
            #t.daemon = True
            self.thread_list.append(t)
            t.start()

if __name__ == "__main__":
    manager = Manager("0.0.0.0", 5005)
    manager.run()

