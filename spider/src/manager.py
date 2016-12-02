#!/usr/bin/python3
#encoding:utf-8

# pylint: disable=superfluous-parens,invalid-name,broad-except

""" master node """

import socket
import datetime
import time
import pickle
import threading
import urllib.parse
import random
from collections import defaultdict
import Bloom_filter
from ConfReader import ConfReader
from unbuffered_output import uopen
from Logger import Logger

#this would cause manager timeout
#socket.setdefaulttimeout(60)

# configuration for manager
default_conf = {
    "links_file":"conf/manager.links",
    "how_many_links_file":"how-many-links.links",
    "links_to_crawler_NR":100,
    "buffer_output":"yes",
    "focus":"yes",
    "crawling_width":10000,
    "speed":"auto",
    "prio_ful_threshold":10000000,
    "DB_url":"127.0.0.1",
    "DB_user":"root",
    "DB_passwd":"root324",
    }

logger = Logger()

class EmptyPriQueue(Exception):
    """ TODO: when would EmptyPriQueue be raise ? """

    def __init__(self, *args):
        super(EmptyPriQueue, self).__init__(*args)

class PriQueue:

    """ a priority queue that used to stored links
        and dispatch links to the right."""

    def __init__(self, links_file):
        """ Initialization """
        self.links = []
        self.links_file = links_file
        #self.randobj = random.Random(7) #use to generate random position for inserting

        # main data structure in this class
        self.links_by_addr = {}       # every crawler use a queue
        self.domains_queue = {}  # which domain correspond to which crawler: domain <=> ip
        self.addr_domainNR = defaultdict(int)    # which crawler has how many domains

        self.dominant_threshold = 20

        self._lock = threading.Lock() #lock with many usages

    def __len__(self):
        """ return how many links this prioQueue has """
        with self._lock:
            total_len = len(self.links)
        for lst in self.links_by_addr:
            total_len = total_len + len(lst)
        return total_len

    def has_pos(self, addr):
        """ check whether crawler from addr has a position in self.links_by_addr """
        if addr in self.links_by_addr:
            return True
        return False

    def set_pos(self, addr):
        """ make a position in self.links_by_addr for crawler(addr) """
        self.links_by_addr.update({addr:[]})

    def _acquire(self):
        self._lock.acquire()

    def _release(self):
        self._lock.release()

    def domains_nr(self):
        """ how many domains this PriQueue have """
        return len(self.domains_queue)

    def get_links_from_disk(self):
        """ Use this to get original links from disk when starting """
        with open(self.links_file, "r") as f:
            for line in f:
                self.links.append(line.strip())
            self.links = list(set(self.links)) #hopefully that the list would get randomized
        logger.info("PriQueue successfully get links from disk\n", Logger.STDOUT)

    def append(self, link):
        """ feed links into the internal prio queue. This would spread all
            the links averagely to every crawler's queue.
            Note that those links are now NOT ordered by prio """
        protocal, domain = self.get_protocal_domain(link)
        self._acquire()   # one lock to make your life easier...
        if domain in self.domains_queue:
            addr = self.domains_queue.get(domain)
            if not self.has_pos(addr):
                self.set_pos(addr)
            #index = int(self.randobj.random() * len(self.links_by_addr[addr]))
            #self.links_by_addr[addr].insert(index, link)
            self.insert_into(addr, link)
        else:
            #select a crawler(addr) to assign this domain to
            sorted_by_val = sorted(self.addr_domainNR.items(), key=lambda x: x[1])
            addr = sorted_by_val[0][0]
            if not self.has_pos(addr):
                self.set_pos(addr)
            #index = int(self.randobj.random() * len(self.links_by_addr[addr]))
            #self.links_by_addr[addr].insert(index, link)
            self.insert_into(addr, link)
            self.domains_queue[domain] = addr
            logger.info("append(): assigning domain[%s] to addr[%s]" % (domain, addr), Logger.STDOUT)
            self.addr_domainNR[addr] = self.addr_domainNR[addr] + 1
        self._release()

    def get_by_addr(self, addr):
        """ get links from specific prio queue """
        #first check whether we still have some links leaf in the original list
        if len(self.links):
            self._acquire()
            link = self.links.pop()
            (protocal, domain) = self.get_protocal_domain(link)
            if not domain in self.domains_queue:
                # assign this domain to this crawler, this might not be
                # even/fair, but it wouldn't affect that much, so ok
                self.domains_queue[domain] = addr
                logger.info("get_by_addr(): assigning domain[%s] to addr[%s]" % (domain, addr),
                        Logger.STDOUT)
                self.addr_domainNR[addr] = self.addr_domainNR[addr] + 1
            self._release()
            return link
        else:
            # all the original links are gone and this crawler still have no position.
            # So we temporarily return a fake link and make a position for it, so that
            # last time when `self.append' is called, this position would be filled
            # with some new links
            self._acquire()
            if not self.has_pos(addr):
                self.set_pos(addr)
                self.addr_domainNR[addr] = 0  # "register" this crawler
                self._release()
                time.sleep(
                    60
                )  # sleep() would halt only current thread, not the entire process
                return 'https://www.oh-my-url-you-are-definitely-going-to-fail.com'
            elif len(self.links_by_addr[addr]):
                logger.info("returning one links to [%s]" % addr, Logger.STDOUT)
                link = self.links_by_addr[addr].pop()
                self._release()
                return link
            else:
                raise EmptyPriQueue("Empty links for crawler:[%s]\n" % addr)

    def insert_into(self, addr, link):
        """ insert this link into proper list, in a proper order """
        # if there are too many link of the same domain in list, then we should
        # remove some so that others links from other domains can have chances
        # to be crawled
        self.links_by_addr[addr].append(link)
        self.links_by_addr[addr] = sorted(self.links_by_addr[addr], key=lambda x: x[::-1])

    def remove_dominant(self):
        """ remove links of dominant domain. A domain is dominant if most
            of links in prio_que are of this domain """
        self._acquire()
        for k, v in self.links_by_addr.items():
            domain_NR = defaultdict(int)
            to_be_del = []
            for index, link in enumerate(v):
                protocal, domain = self.get_protocal_domain(link)
                if domain_NR[domain] <= self.dominant_threshold:
                    domain_NR[domain] = domain_NR[domain] + 1
                else:
                    to_be_del.append(index)
            new_v = []
            for index, link in enumerate(v):
                if index not in to_be_del:
                    new_v.append(link)
            self.links_by_addr[k] = new_v
        self._release()

    def get_protocal_domain(self, url):
        """ return protocal and domain """
        protocal, rest = urllib.parse.splittype(url)
        domain, url_suffix = urllib.parse.splithost(rest)
        return (protocal, domain)

class Manager:
    """Core manager """

    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        """ Initialization """

        self.conf = ConfReader("manager.conf", default_conf)

        self.my_open = uopen if self.conf.get("buffer_output") == "no" else open
        self.links_file = self.conf.get("links_file")
        self.how_many_links_file = self.conf.get("how_many_links_file")
        #record all the links we have crawled
        self._links_track = self.my_open(self.how_many_links_file, "w+")
        self._links_track_lock = threading.Lock()   #lock
        #how many links we send to crawler per request
        self._nsent = self.conf.get("links_to_crawler_NR")

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
        self.prio_ful_threshold = self.conf.get("prio_ful_threshold")

        self.thread_list = []        #list of threads in manager
        self.thread_num = thread_num #how many thread we should start

        self.auto_speed = True
        self.speed_count = 0  # how many links to crawl from a website. Not using now
        speed = self.conf.get("speed")
        if speed != "auto":   # the art of dynamic language
            self.speed_count = speed
            self.auto_speed = False

        self.focusing = False if self.conf.get("focus") == "no" else True

        # crawling_width take effect when self.focusing is False
        self.crawling_width = self.conf.get("crawling_width")

        #MACRO,represent whether crawler want to send back links
        #or get links from here
        self.SEND = 0
        self.REQUEST = 1
        self.ASKFOCUSING = 2

    def bf_acquire(self):
        self.bf_lock.acquire()

    def bf_release(self):
        self.bf_lock.release()

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
                        self.bf_acquire()
                        #加入bloom_filter之中，否则这个链接会被重爬
                        self.bloom_filter.add(key)
                        self.bf_release()
                    else:
                        self.bf_acquire()
                        crawled_links.append(key)
                        self.bloom_filter.add(key)
                        # limit links count(it's ok to exceed a little bit)
                        if len(self.prio_que) < self.prio_ful_threshold:
                            for sub_link in value:
                                if sub_link not in self.bloom_filter:
                                    self.prio_que.append(sub_link)
                            # remove links of dominant domain so that links
                            # from other domains have an opportunity to be
                            # crawled
                            self.prio_que.remove_dominant()
                        self.bf_release()

                #write all the link to `self._links_track`
                with self._links_track_lock:
                    for link in crawled_links:
                        self._links_track.write(str(link) + "\n")

            elif (method == self.REQUEST): #crawler request some links
                conn.sendall(b'OK')
                if (self.crawling_width < self.prio_que.domains_nr()):
                    self.focusing = False  # we have enogh domains now, so not focusing anymore
                    logger.info("Not focusing anymore. crawling_width[%d],"
                            "domains_nr[%d]\n" % (self.crawling_width, self.prio_que.domains_nr()),
                            Logger.STDOUT)
                #crawler would ask whether or not to be focusing
                if self.get_conn_type(conn) == self.ASKFOCUSING:
                    if self.focusing:
                        logger.info("manager sending back FOCUSING message\n",
                                Logger.STDOUT)
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
            logger.info("Exception:[%s]" % str(e), Logger.STDERR)
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
        logger.info("manager start running at: [%s]\n" % str(datetime.datetime.now()),
                Logger.STDOUT)
        while(True):
            #only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            logger.info("Connection established: %s\n" % str(addr), Logger.STDOUT)
            t = threading.Thread(target=self.handle_connection, args=(conn, addr))
            #t.daemon = True
            self.thread_list.append(t)
            t.start()

if __name__ == "__main__":
    manager = Manager("0.0.0.0", 5005)
    manager.run()

