#!/usr/bin/python3
# -*- coding: UTF-8 -*-
from DNS_Resolver.DNS_Resolver import DNSReslover
import requests
from lxml import etree
import re
from queue import Queue
import threading
from multiprocessing.dummy import Pool as ThreadPool

class Crawler(object):

    def __init__(self):
        self.thread_pool_size = 4
        self.thread_pool = ThreadPool(self.thread_pool_size)
        self._queue = Queue()
        self._result_dict = {}
        self._log = open("crawler.log","w+")
        self._lock = threading.Lock()
        self.dns_resolver = DNSReslover()
        self.result_sender = None

    def get_web(self,resolved_url):
       """This function used for grab a web information and return a Response object."""
       #TODO: we can fake as google. there should be
       #应该有一个ACCEPT:xxx说明我们只需要文本
       headers = {'User-Agent': 'XXX', 'Referer': 'XXX'}  
       proxies = {'http': 'http://XX.XX.XX.XX:XXXX'}
       try:
           response = requests.get(resolved_url, headers = headers, proxy = proxies)
           if (response.status_code == 200):
               return response
           elif(response.status_code == 302):
               return None  #maybe we want to handle redirection
           else:
               return None
       except Exception as e:
           with self._lock: #避免race condition
               self._log.write("Fail to fetch resolved_url. Exception: %s\n", str(e)) 
           return None
   
    def run(self):
        """ run the crawler
        @urls: a dict, used to hold the raw urls get from the left.  """
        while (True):
            try:
                #a dict of format { url1 => resolved_url,
                #                   url2 => resolved_url,
                #                   ...}
                url_dict = self.dns_resolver.get_resolved_url_packet()
            except Exception as e:
                #TODO: handle exception
                pass
            if not urls:
                #TODO: handler connection failure
                pass
            #将解析成功的和未成功的分开来
            resolved_dict = {}
            fail_resolved_dict = {}
            for key, value in url_dict.items():
                if value:
                    resolved_dict[key] = value
                else:
                    fail_resolved_dict[key] = None 

            #未解析成功的就直接是FAIL了
            for key, value in fail_resolved_dict.items():
                self._result_dict[key] = "FAIL"

            #处理解析成功的
            responses = self.thread_pool.map(self.get_web, resolved_dict.values())
            #close the pool and wait for all the work to finish
            self.thread_pool.join()
            original_urls = resolved_dict.keys()
            for index, resp in enumerate(responses):
                origin = original_urls[index]
                if not resp:
                    self._result_dict[origin] = "FAIL"
                else:
                    text = self.change_to_string(resp)
                    outer_links, inner_links = self.extract_link(origin, resp)
                    outer_links = set(outer_links)
                    inner_links = set(inner_links)
                    self._result_dict[original_urls[index]]
            
            #将东西返回给左边
            #self.result_sender.send(self._result_dict)
            #self._result_dict = {}
                    
            
    def change_to_string(self, response):
        """Change the Response object to string and return it"""
        html_text = etree.HTML(response.text)
        return etree.tostring(html_text, pretty_print=True)

    def extract_link(self, origin_url, html):
        """This function is used for extract all links from the web.
           Use the href attributes, Return the result."""
        html_text = etree.HTML(html)
        results = html_text.xpath('//@href')
        return self.handle_link(origin_url, results)

    def handle_link(self, origin_url, results):
        """The function is used for deal with the links.
           Distinct the inner links and outer links.
           For inner links, it should add the header and
           delete the tag#, remove .css and javascript link"""
        # distinct inner from outer link through the header http
        pattern = re.compile(r'http://|https://')
        # get the url domain to define the website 
        # TODO: domain name regex may need modification
        domain_pattern = re.compile(r'http://[\w+\.]+|https://[\w+\.]+')
        domain = re.match(domain_pattern, origin_url).group()
        # define the .css or javascript file
        useless_pattern = re.compile(r'/|javascript|\S*.css')
        # define the tag#
        tag_pattern = re.compile(r'\S*#\S*')
        outer_link_lists = []
        inner_link_lists = []
        for element in results:
            match = re.match(pattern,element)
            if match:  # begin with http
                test_domain = re.match(domain_pattern, element).group()  # test the header for spcific definition
                if test_domain != domain:
                    outer_link_lists.append(match.string)
                else:  # same domain
                    inner_link_lists.append(element)
            else:  # not begin with http
                test_inner = re.match(useless_pattern,element)  # test if it's a css or javascript file
                if not test_inner:
                    test_tag = re.match(ttag_pattern,element)
                    if test_tag:  # test if it's a page tag#
                        pass
                    else:
                        link = domain + '/' + element
                        inner_link_lists.append(link)
                else:
                    pass

        return (outer_link_lists, inner_link_lists)

