#!/usr/bin/python3
# -*- coding: UTF-8 -*-

""" crawler node """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import re
from concurrent.futures import ThreadPoolExecutor
import pickle
import time
import urllib.parse
import traceback
import requests
from lxml import etree
from NetworkHandler import NetworkHandler
import readability.readability
from unbuffered_output import uopen
from ConfReader import ConfReader
from Logger import Logger
from DBUtil import DBHandler

#default configurations for crawler
default_conf = {
    "manager_ip":"127.0.0.1",
    "manager_port":5005,
    "content_path":"dump",
    "buffer_size_threshold":50,
    "concurrent_crawl_NR":50,
    "buffer_output":"yes",
    "thread_pool_size":1000,
    "crawling_timeout":120,
    "DB_url":"127.0.0.1",
    "DB_user":"root",
    "DB_passwd":"root324",
    }

class Crawler(object):
    """ The crawler.
        Multiple threads would be started in method run() """

    def __init__(self):
        """ Initialization """

        self.conf = ConfReader("crawler.conf", default_conf)
        self.log = Logger()
        self.db = None

        self.thread_pool_size = self.conf.get("thread_pool_size")
        self.left_ip = self.conf.get("manager_ip")
        self.left_port = self.conf.get("manager_port")
        self._buffer_size_threshold = self.conf.get("buffer_size_threshold")
        #how many links we should return when the caller call self.get_links()
        self._crawl_NR = self.conf.get("concurrent_crawl_NR")
        self.my_open = uopen if self.conf.get("buffer_output") == "no" else open
        self.content_path = self.conf.get("content_path")
        self.crawling_timeout = self.conf.get("crawling_timeout")

        DB_url = self.conf.get("DB_url")
        DB_user = self.conf.get("DB_user")
        DB_passwd = self.conf.get("DB_passwd")
        self.db = DBHandler("crawlerDB", DB_user, DB_passwd, DB_url)
        self.db.connect()
        self.db.update("CREATE TABLE IF NOT EXISTS `pages_table` ("
                       " `page_id` int(20) NOT NULL AUTO_INCREMENT,"
                       " `page_url` varchar(400) NOT NULL,"
                       " `domain_name` varchar(100) NOT NULL,"
                       #" `sublinks` text,"
                       " `title` varchar(255),"
                       #" `normal_content` text,"
                       #" `emphasized_content` text,"
                       " `keywords` varchar(255),"
                       " `description` varchar(511),"
                       " `text` longtext,"
                       " `PR_score` double default 0.0,"
                       " `ad_NR` int default 0,"
                       #" `classify_attribute` ...
                       " PRIMARY KEY (`page_id`)"
                       ") ENGINE=InnoDB" )


        # hold all the links to be sent back to manager
        self._result_dict = {}
        # used to hold all the links which are got from manager
        self._buffer = []
        self.result_sender = NetworkHandler(self.left_ip, self.left_port)
        self.links_requester = NetworkHandler(self.left_ip, self.left_port)
        self.focusing = True  # whether or not the crawling should do focus-crawling


    def get_links(self):
        """ used to get urls from manager.
        we use a buffer, so that we can get 50 links from manager,
        and then return 10 links with call to self.get_links() one by one.
        To do this, for example, user can adjust the 'concurrent_crawl_NR'
        setting in 'conf/crawler.conf' to 10 and 'links_to_crawler_NR' to 50.

        Note that we don't have to set any timeount here,
        because, after all, crawler have to get some links from
        manager side before it can continue """

        # if there are not enough links in the buffer
        if len(self._buffer) < self._buffer_size_threshold:
            try:
                # manager would return links together with a
                # message(self.focusing), which tell the crawler whether it
                # should still be focused-crawling or not
                (self.focusing, links) = self.links_requester.request()
                self.log.info("links_requester fail request()")
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
            self.log.info("Get response[%d]: [%s]" % (response.status_code, resolved_url))
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
            self.log.info("Fail to fetch page. Exception: %s, url:[%s]" % (str(e), resolved_url))
            return None

    def run(self):
        """ main routine of crawler class
            @urls: used to hold the raw urls got from the left.  """

        while (True):
            try:
                urls = self.get_links()
            except Exception as e:
                self.log.info("Cannot get urls. crawler sleep for 10 seconds."
                        "Exception:[%s]\n" % str(e))
                time.sleep(10) #wait a little bit to see if thing would get better
                continue
            if not urls:
                self.log.info("Empty urls from dns_resolver. Crawler exit")
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
                    except Exception as e:
                        self.log.info("Exception when extract_links:[%s],"
                                "url:[%s]\n" % (str(e), origin))
                        continue
                    self.log.info("Finished extract_links()")
                    outer_links = set(outer_links) #outer_links not handled yet
                    inner_links = set(inner_links)
                    if self.focusing:
                        self.log.info("crawler is FOCUSING now.\n")
                        self._result_dict[origin] = inner_links
                    else:
                        self._result_dict[origin] = inner_links.union(outer_links)

                    # resp.content return 'bytes' object
                    try:
                        self.dump_content(resp)
                    except Exception as e:
                        self.log.info("Exception when dump_content():[%s],"
                                "url:[%s]" % (str(e), origin))
                        traceback.print_exc()
                        continue
                    self.log.info("Finished dump_content()")

            data = pickle.dumps(self._result_dict)
            try:
                self.result_sender.send(data)
                self.log.info("successfully sent back to the left\n")
            except Exception as e:
                self.log.info(("Fail sending to manager:[%s]\n"
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

    def dump_content(self, resp):
        """ requests cannot detect web page encoding automatically(FUCK!).
            response.encoding is from the html reponse header. If we want to
            convert all the content we want to utf8, we have to use `get_encodings_from_content;' """
        # resp.text is in unicode(type 'str')
        try:
            real_encoding = requests.utils.get_encodings_from_content(resp.text)[0]
            text = resp.content.decode(real_encoding).encode('utf-8')
        except Exception:
            text = resp.content

        def extract_kw_desc(text):
            kws = re.findall(rb'<meta\s+name\s{0,2}=\s{0,2}"keywords"\s+content\s{0,2}=\s{0,2}"(.{0,255}?)" />',
                    text, re.DOTALL)
            descs = re.findall(rb'<meta\s+name\s{0,2}=\s{0,2}"description"\s+content\s{0,2}=\s{0,2}"(.{0,511}?)" >',
                    text, re.DOTALL)
            kw = b""
            if kws:
                kw = kws[0]
            desc = b""
            if descs:
                desc = descs[0]
            return kw, desc

        page_url = bytes(resp.url, 'utf-8')
        _, domain_name = self.get_protocal_domain(resp.url)
        domain_name = bytes(domain_name, 'utf-8')
        titles = re.findall(rb'<title>(.*?)</title>', text)
        title = b""
        if titles:
            title = titles[0]
        kw, desc = extract_kw_desc(text)

        self.db.update("INSERT INTO pages_table (`page_url`, `domain_name`,"
                "`title`, `text`, `keywords`, `description`) "
                "VALUES (%s, %s, %s, %s, %s, %s);",
                (page_url, domain_name, title, text, kw, desc))


if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()

