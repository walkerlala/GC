#!/usr/bin/python3 -u
# -*- coding: UTF-8 -*-

""" crawler node """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import re
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle
import time
import datetime
import urllib.parse
from DNS_Resolver import DNSResolver
import requests
from lxml import etree
from NetworkHandler import NetworkHandler
from unbuffered_output import uopen

left_ip = '119.29.166.19'
left_port = 5005    #port have to be of type int, not str. f**k

log = uopen("crawler.log", "w+")
lock = threading.Lock()  #lock to log file

def get_web(resolved_url): #*args would refer to (log, lock)
    """This function used for grab a web information and return a Response object."""

    #fake as YoudaoBot. Can also fake as GoogleBot, or Baidu Spider,
    #but this maybe easily detect due to ip-mismatch
    #NOTE: According to RFC 7230, HTTP header names are case-insensitive
    headers = {'User-Agent': 'YoudaoBot',
               'Accept':'text/plain, text/html', #want only text
               #'Accept-Encoding':"",  Requests would handle encoding and decoding for us
              }
    try:
        response = requests.get(resolved_url, headers=headers, timeout=120)
        print("Get response[%d]" % response.status_code)
        with lock:
            log.write("Response[%d] of url:[%s]\n" % (response.status_code, resolved_url))
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
        with lock: #避免race condition
            print("Fail to fetch resolved_url. exception: %s" % str(e))
            log.write("Fail to fetch resolved_url. Exception: %s\n" % str(e))
        return None

class Crawler(object):
    """ The crawler. Multiple thread would be started in method run() """

    def __init__(self):
        self.thread_pool_size = 4
        #self.thread_pool = ThreadPool(self.thread_pool_size)
        self._result_dict = {}
        self.dns_resolver = DNSResolver(left_ip, left_port)
        #TODO: we may set timeout for result_sender(but not for links_requester)
        self.result_sender = NetworkHandler(left_ip, left_port)

    def run(self):
        """ main routine of crawler class
            @url_dict: a dict, used to hold the raw urls get from the left.  """
        while (True):
            try:
                #a dict of format { url1 => resolved_url, url2 => resolved_url, ...}
                url_dict = self.dns_resolver.get_resolved_url_packet()
            except Exception as e:
                log.write("Cannot get resolved url packet. Exception:[%s]\n" % str(e))
                time.sleep(0.5) #wait a little bit to see if thing would get better
                continue
            if not url_dict:
                t = str(datetime.datetime.now())
                print("[%s]Empty urls from dns_resolver. Crawler exit\n" % t)
                log.write("[%s]Empty urls from dns_resolver. Crawler exit\n" % t)
                break

            #将解析成功的和未成功的分开来
            resolved_dict = {}
            fail_resolved_dict = {}
            for key, value in url_dict.items():
                if value:
                    resolved_dict[key] = value
                else:
                    fail_resolved_dict[key] = 'FAIL'

            #未解析成功的就直接是FAIL了
            self._result_dict.update(fail_resolved_dict)

            #处理解析成功的
            log.write("Get resolved_dict:[%s]\n" % str(resolved_dict))
            with ThreadPoolExecutor(1000) as pool:
                responses = pool.map(get_web, resolved_dict.values())

            #开始处理response，将得到的子内链与源链接组合在一起然后返回
            original_urls = list(resolved_dict.keys())
            for index, resp in enumerate(responses):
                origin = original_urls[index]
                if not resp:
                    self._result_dict[origin] = "FAIL"
                else:
                    #Note that we only accept a response of type 'text/html' here
                    #Note that resp.text return unicode string
                    try:
                        outer_links, inner_links = self.extract_link(origin, resp.text)
                    except Exception as e:
                        log.write(
                            "Exception when extract_links:[%s], url:[%s]\n" % (str(e), origin))
                        continue
                    outer_links = set(outer_links)  #outer_links not handled yet
                    inner_links = set(inner_links)
                    self._result_dict[origin] = inner_links

            #将东西返回给左边
            #对self._result_dict做serialization，以便可以在TCP通道中传输
            print("sending back things to the left...")
            #just a test, to see if we get some thing useful
            with lock:
                log.write("self._result_dict:[%s]\n" % str(self._result_dict))

            data = pickle.dumps(self._result_dict)
            try:
                self.result_sender.send(data)
                print("successfully sent back to the left")
            except Exception as e:
                print("exception happen when sent things back to the left:[%s]" % str(e))
                log.write(("Fail sending to Manager:[%s]\n"
                                    "unsent links:[%s]\n") % (str(e), str(self._result_dict)))
            finally:
                self._result_dict = {}

    def extract_link(self, origin_url, html):
        """This function is used for extract all links from the web.
           Distinct the inner links and outer links.
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
        host, url_suffix = urllib.parse.splithost(rest)
        return (protocal, host)

if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()

