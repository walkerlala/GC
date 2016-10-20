#!/usr/bin/python3
#encoding:utf-8

# pylint: disable=superfluous-parens,invalid-name,broad-except

""" master node """

import socket
import datetime
import pickle
import threading
import urllib.parse
import random
from collections import defaultdict
import Bloom_filter
from ConfReader import ConfReader
from unbuffered_output import uopen

#this would cause manager timeout
#socket.setdefaulttimeout(60)

#FIXME manager should remove its own previous file, and create necessary directory
default_conf = {
    "links_file":"conf/manager.links",
    "how_many_links_file":"how-many-links.links",
    "log_path":"log",
    "links_to_crawler_NR":100,
    "buffer_output":"yes",
    "focus":"yes",
    "crawling_width":10000,
    "speed":"auto",
    "prio_ful_threshold":10000000,
    }


class EmptyPriQueue(Exception):
    def __init__(self, *args):
        super(EmptyPriQueue, self).__init__(*args)

class PriQueue:
    """ a priority queue that used to stored links
        and dispatch links to the right."""

    def __init__(self, links_file):
        self.links = []
        self.links_by_addr = {}       # every crawler use a prio queue
        self.links_file = links_file
        self.randobj = random.Random(7) #use to generate random position for inserting
        self.pr_lock = threading.Lock() #lock to access priority queue
        self.domains_queue = {}  # which domain correspond to which crawler: domain <=> ip
        self.addr_domainNR = defaultdict(int)    # which crawler has how many domains

    def __len__(self):
        with self.pr_lock:
            total_len = len(self.links)
            for lst in self.links_by_addr:
                total_len = total_len + len(lst)
            return total_len

    def domains_nr(self):
        """ how many domains do this PriQueue have """
        return len(self.domains_queue)

    def get_links_from_disk(self):
        """ This is test method. Use this to get links from disk,
            as we have not implemented the prio method """
        with open(self.links_file, "r") as f:
            for line in f:
                self.links.append(line.strip())
            self.links = list(set(self.links)) #hopefully that the list would get randomized
        print("PriQueue successfully get links from disk\n")

    def append(self, link):
        """ feed links into the internal prio queue. This would spread all
            the links averagely to every crawler's queue.
            Note that those links are now NOT ordered by prio """
        protocal, domain = self.get_protocal_domain(link)
        if domain in self.domains_queue:
            addr = self.domains_queue.get(domain)
            if not self.has_pos(addr):
                self.set_pos(addr)
            index = int(self.randobj.random() * len(self.links_by_addr[addr]))
            self.links_by_addr[addr].insert(index, link)
        else:
            #select a crawler(addr) to assign this domain to
            sorted_by_val = sorted(self.addr_domainNR.items(), key=lambda x: x[1])
            addr = sorted_by_val[0][0]
            if not self.has_pos(addr):
                self.set_pos(addr)
            index = int(self.randobj.random() * len(self.links_by_addr[addr]))
            self.links_by_addr[addr].insert(index, link)
            self.domains_queue[domain] = addr
            print("append(): assigning domain[%s] to addr[%s]" % (domain, addr))
            self.addr_domainNR[addr] = self.addr_domainNR[addr] + 1

    def get_by_addr(self, addr):
        """ get links from specific prio queue """
        #first check whether we still have some links leaf in the original list
        if len(self.links):
            link = ""
            with self.pr_lock:
                link = self.links.pop()
            (protocal, domain) = self.get_protocal_domain(link)
            if not domain in self.domains_queue:
                # assign this domain to this crawler, this might not be
                # even/fair, but it wouldn't affect that much, so ok
                self.domains_queue[domain] = addr
                print("get_by_addr(): assigning domain[%s] to addr[%s]" % (domain, addr))
                self.addr_domainNR[addr] = self.addr_domainNR[addr] + 1
            return link
        else:
            # all the original links are gone and this crawler still have no position.
            # So we temporarily return a fake link and make a position for it, so that
            # last time when `self.append' is called, this position would be filled
            # with some new links
            if not self.has_pos(addr):
                self.set_pos(addr)
                self.addr_domainNR[addr] = 0  # "register" this crawler
                return 'https://www.oh-my-url-you-are-definitely-going-to-fail.com'
            elif len(self.links_by_addr[addr]):
                print("returning one links...")
                return self.links_by_addr[addr].pop()
            else:
                raise EmptyPriQueue("Empty links for crawler:[%s]\n" % addr)

    def has_pos(self, addr):
        """ check whether crawler from addr has a position in self.links_by_addr """
        if addr in self.links_by_addr:
            return True
        return False

    def set_pos(self, addr):
        self.links_by_addr.update({addr:[]})

    def get_protocal_domain(self, url):
        """ return protocal and domain """
        protocal, rest = urllib.parse.splittype(url)
        domain, url_suffix = urllib.parse.splithost(rest)
        return (protocal, domain)

class Manager:
    """Core manager """

    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        """ Initialization """

        self.conf = ConfReader("manager.conf")

        tmp = self.conf.get("unbuffered_output")
        if tmp == "no":
            print("manager: un-buffer output")
            self.my_open = uopen
        else:
            self.my_open = open

        tmp = self.conf.get("links_file")
        self.links_file = tmp if tmp else default_conf["links_file"]

        tmp = self.conf.get("how_many_links_file")
        self.how_many_links_file = tmp if tmp else default_conf["how_many_links_file"]
        #record all the links we have crawled
        self._links_track = self.my_open(self.how_many_links_file, "w+")
        self._links_track_lock = threading.Lock()   #lock

        tmp = self.conf.get("log_path")
        self.log_path = tmp if tmp else default_conf["log_path"]
        self._log = self.my_open(self.log_path + "/manager.log", "w+") #log file
        self.log_lock = threading.Lock()       #lock to access log file

        #how many links we send to crawler per request
        tmp = self.conf.get("links_to_crawler_NR")
        self._nsent = tmp if tmp else default_conf["links_to_crawler_NR"]

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

        self.prio_que = PriQueue(self.links_file)          #manager's priority queue
        self.prio_que.get_links_from_disk() #initially get links from disk
        #self.domains_nr = self.prio_que.domains_nr()
        tmp = self.conf.get("prio_ful_threshold")
        self.prio_ful_threshold = tmp if tmp else default_conf["prio_ful_threshold"]

        self.thread_list = []        #list of threads in manager
        self.thread_num = thread_num #how many thread we should start

        self.auto_speed = True
        self.speed_count = 0
        tmp = self.conf.get("speed")
        speed = tmp if tmp else default_conf["speed"]
        if speed != "auto":
            self.speed_count = speed
            self.auto_speed = False

        tmp = self.conf.get("focus")
        focusing = tmp if tmp else default_conf["focus"]
        self.focusing = False if focusing == "no" else True

        # crawling_width take effect when self.focusing is False
        tmp = self.conf.get("crawling_width")
        self.crawling_width = tmp if tmp else default_conf["crawling_width"]

        #MACRO,represent whether crawler want to send back links
        #or get links from here
        self.SEND = 0
        self.REQUEST = 1
        self.ASKFOCUSING = 2

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
                            # limit links count(it's ok to exceed a little bit)
                            if len(self.prio_que) < self.prio_ful_threshold:
                                for sub_link in value:
                                    if sub_link not in self.bloom_filter:
                                        self.prio_que.append(sub_link)
                #write all the link to `self._links_track`
                with self._links_track_lock:
                    for link in crawled_links:
                        self._links_track.write(str(link) + "\n")

            elif (method == self.REQUEST): #crawler request some links
                conn.sendall(b'OK')
                if (self.crawling_width < self.prio_que.domains_nr()):
                    self.focusing = False  # we have enogh domains now, so not focusing anymore
                    self._log.write("Not focusing anymore. crawling_width[%d],"
                            "domains_nr[%d]\n" % (self.crawling_width, self.prio_que.domains_nr()))
                #crawler would ask whether or not to be focusing
                if self.get_conn_type(conn) == self.ASKFOCUSING:
                    if self.focusing:
                        self._log.write("manager sending back FOCUSING message\n")
                        conn.sendall(b'OK')
                    else:
                        conn.sendall(b'NO') # can only send back two bytes

                """ 假如prioQueue里面没有了就会返回一个空的lists """
                data = None
                links_buffer = []
                try:
                    for _ in range(self._nsent):  #一次发送多少self._nsent条链接
                        links_buffer.append(self.prio_que.get_by_addr(addr[0]))
                except EmptyPriQueue:
                    pass
                except Exception as e:
                    raise Exception("Exception:[%s] when getting links from PriQueue" % str(e))
                #如果prio_que里面没有链接了，我们发送过去的就是一个空的list了
                data = pickle.dumps(links_buffer)
                try:
                    conn.sendall(data)
                except Exception as e:
                    raise
            else:
                raise Exception("UNKNOWN CONNECTION TYPE")
        except Exception as e:
            #不能再向上抛异常了，因为这是多线程模型，
            #异常应该在本函数内处理
            with self.log_lock:
                t = str(datetime.datetime.now())
                self._log.write("[%s]::Exception:[%s]\n" % (t, str(e)))
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
            elif (data == b'FOCUSING?'):
                return self.ASKFOCUSING
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

