#!/usr/bin/python3
# -*- coding: UTF-8 -*-

""" crawler node """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import re
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle
import time
import datetime
import urllib.parse
import requests
from lxml import etree
from NetworkHandler import NetworkHandler
import readability.readability
from unbuffered_output import uopen
from ConfReader import ConfReader

#default configurations
default_conf = {
    "manager_ip":"127.0.0.1",
    "manager_port":5005,
    "log_path":"log",
    "content_path":"dump",
    "buffer_size_threshold":50,
    "concurrent_crawl_NR":50,
    "buffer_output":"yes",
    "thread_pool_size":1000,
    "crawling_timeout":120,
    }

class Crawler(object):
    """ The crawler.
        Multiple threads would be started in method run() """

    def __init__(self):
        """ Initialization """

        self.conf = ConfReader("crawler.conf")

        tmp = self.conf.get("thread_pool_size")
        self.thread_pool_size = tmp if tmp else default_conf["thread_pool_size"]

        tmp = self.conf.get("manager_ip")
        self.left_ip = tmp if tmp else default_conf["manager_ip"]

        tmp = self.conf.get("manager_port")
        self.left_port = tmp if tmp else default_conf["manager_port"]

        tmp = self.conf.get("buffer_size_threshold")
        self._buffer_size_threshold = tmp if tmp else default_conf["buffer_size_threshold"]

        #how many links we should return when the caller call self.get_links()
        tmp = self.conf.get("concurrent_crawl_NR")
        self._crawl_NR = tmp if tmp else default_conf["concurrent_crawl_NR"]

        tmp = self.conf.get("buffer_output")
        buffer_output = tmp if tmp else default_conf["buffer_output"]
        if buffer_output == "no":
            self.my_open = uopen
        else:
            self.my_open = open

        tmp = self.conf.get("content_path")
        self.content_path = tmp if tmp else default_conf["content_path"]
        tmp = self.conf.get("log_path")
        self.log_path = tmp if tmp else default_conf["log_path"]
        self.log = self.my_open(self.log_path + "/crawler.log", "w+")
        #lock for log
        self.lock = threading.Lock()

        tmp = self.conf.get("crawling_timeout")
        self.crawling_timeout = tmp if tmp else default_conf["crawling_timeout"]

        # hold all the links to be sent back to manager
        self._result_dict = {}
        # used to hold all the links which are got from manager
        self._buffer = []
        self.result_sender = NetworkHandler(self.left_ip, self.left_port)
        self.links_requester = NetworkHandler(self.left_ip, self.left_port)
        self.focusing = True  # whether or not the crawling should do focus-crawling

        self.html_count = 0 # how many html page have been downloaded

        # FIXME crawler should remove its own file, not manager's, in case the
        # two are on the same machine
        # make sure all those directories exist and clear
        if not os.path.exists(self.content_path):
            os.mkdir(self.content_path)
        else:
            list(map(os.unlink,
                     (os.path.join(self.content_path, f)
                         for f in os.listdir(self.content_path) if f.find("crawler"))))
        if not os.path.exists(self.log_path):
            os.mkdir(self.log_path)

    def get_links(self):
        """ used to get urls from manager.
        Note that we don't have to set any timeount here,
        because, after all, crawler have to get some links from
        manager side before it can continue """

        # if there are not enough links in the buffer
        if len(self._buffer) < self._buffer_size_threshold:
            try:
                (self.focusing, links) = self.links_requester.request()
                if not links:
                    #return whatever in self._buffer
                    tmp = self._buffer
                    self._buffer = []
                    return tmp
                else:
                    self._buffer.extend(links)
                    #make sure that we don't exceed the limit
                    nsent = (self._crawl_NR if self._crawl_NR <= len(self._buffer)
                                            else len(self._buffer))
                    tmp = []
                    for _ in range(nsent):
                        tmp.append(self._buffer.pop())
                    return tmp
            except Exception:
                raise
        else:
            #make sure that we don't exceed the limit
            nsent = (self._crawl_NR if self._crawl_NR <= len(self._buffer)
                                    else len(self._buffer))
            #we have enough links, so just return
            tmp = []
            for _ in range(nsent):
                tmp.append(self._buffer.pop())
            return tmp

    def get_web(self, resolved_url):
        """used to grab a web information and return a Response object."""

        #fake as 'Baidu Spider'. Can also fake as GoogleBot, or YoudaoBot,
        #but this maybe easily detected due to ip-mismatch
        #NOTE: According to RFC 7230, HTTP header names are case-INsensitive
        headers = {'User-Agent': 'Baidu Spider',
                   'Accept':'text/plain, text/html', #want only text
                   #' Requests would handle encoding and decoding for us
                  }
        try:
            response = requests.get(resolved_url, headers=headers, timeout=self.crawling_timeout)
            print("Get response[%d]: [%s]" % (response.status_code, resolved_url))
            with self.lock:
                self.log.write("Get response[%d], [%s]\n" % (response.status_code, resolved_url))
            #check whether we get a plain text response
            #note that key in `response.headers` is case insensitive
            if 'content-type' in response.headers:
                if 'text/' not in response.headers['content-type']:
                    return None
            if (response.status_code == requests.codes.ok): #200
                return response
            else:
                return None
        except Exception as e:
            with self.lock: #避免race condition
                self.log.write("Fail to fetch page. Exception: %s\n" % str(e))
            return None

    def run(self):
        """ main routine of crawler class
            @urls: used to hold the raw urls got from the left.  """

        while (True):
            try:
                urls = self.get_links()
            except Exception as e:
                self.log.write("Cannot get urls. Exception:[%s]\n" % str(e))
                time.sleep(0.5) #wait a little bit to see if thing would get better
                continue
            if not urls:
                t = str(datetime.datetime.now())
                self.log.write("[%s]Empty urls from dns_resolver. Crawler exit\n" % t)
                break

            # 爬取链接
            with ThreadPoolExecutor(self.thread_pool_size) as pool:
                responses = pool.map(self.get_web, urls)

            #开始处理response，将得到的子内链与源链接组合在一起然后返回
            for index, resp in enumerate(responses):
                origin = urls[index]
                if not resp:
                    self._result_dict[origin] = "FAIL"
                else:
                    try:
                        # Note that we resp is already of type 'text/html'
                        # Note that resp.text return unicode string
                        outer_links, inner_links = self.extract_link(origin, resp.text)
                        outer_links = set(outer_links) #outer_links not handled yet
                        inner_links = set(inner_links)
                        if self.focusing:
                            self.log.write("crawler is FOCUSING now.\n")
                            self._result_dict[origin] = inner_links
                        else:
                            self._result_dict[origin] = inner_links.union(outer_links)

                        self.dump_content(origin, resp.text)

                    except Exception as e:
                        self.log.write("Exception when extract_links:[%s],"
                                "url:[%s]\n" % (str(e), origin))
                        continue

            data = pickle.dumps(self._result_dict)
            try:
                self.result_sender.send(data)
                print("successfully sent back to the left")
                self.log.write("successfully sent back to the left\n")
            except Exception as e:
                self.log.write(("Fail sending to manager:[%s]\n"
                                    "unsent links:[%s]\n") % (str(e), str(self._result_dict)))
            finally:
                self._result_dict = {}

    def extract_link(self, origin_url, html):
        """This function is used for extract all links from the web.
           It would distinct the inner links and outer links.
           For inner links, it should add the header and
           delete the tag#, remove .css and javascript link"""
        html_text = etree.HTML(html)
        links = html_text.xpath('//*/a/@href') #all the links, relative or absolute

        origin_url = origin_url.strip()
        # get the url domain to define the website
        protocal, domain = self.get_protocal_domain(origin_url)

        #useless file pattern (something like xxx.jpg, xxx.mp4, xxx.css, xxx.pdf, etc)
        uf_pattern = re.compile(r'\..{0,5}')
        #unsupported protocal pattern(something like ftp://, sftp://, thunders://, etc)
        up_pattern = re.compile(r'^.{0,8}:')
        #tag link pattern
        tag_pattern = re.compile(r'\S*#\S*')

        outer_link_lists = []
        inner_link_lists = []
        #we only support http/https protocal
        sp_pattern = re.compile(r'http://|https://')
        for element in links:
            element = element.strip()
            if re.match(sp_pattern, element):  # begin with http/https
                #first check if this match those useless pattern
                if re.findall(uf_pattern, element) or re.findall(tag_pattern, element):
                    continue
                #check whether it's outer link or inner link
                test_protocal, test_domain = self.get_protocal_domain(element)
                if test_domain != domain:
                    outer_link_lists.append(element)
                else:
                    inner_link_lists.append(element)
            elif re.findall(uf_pattern, element):
                continue
            elif re.findall(tag_pattern, element):
                continue
            elif re.findall(up_pattern, element):
                continue
            else:
                if element.startswith('/'):
                    link = protocal + '://' + domain + element
                else:
                    link = protocal + '://' + domain + '/' + element
                inner_link_lists.append(link)

        return (outer_link_lists, inner_link_lists)

    def get_protocal_domain(self, url):
        """ return protocal and domain """
        protocal, rest = urllib.parse.splittype(url)
        domain, url_suffix = urllib.parse.splithost(rest)
        return (protocal, domain)

    def dump_content(self, url, text):
        # text is in unicode
        doc = readability.Document(text)
        # doc is in utf-8
        title = doc.title()
        summary_in_html = doc.get_clean_html()
        file = self.my_open(self.content_path + "/" + str(self.html_count), "w")
        file.write(url.strip() + "\n")
        file.write(title.strip() + "\n")
        file.write(summary_in_html)
        self.html_count = self.html_count + 1
        file.close()

if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()

