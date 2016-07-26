#!/usr/bin/python3 -u
# -*- coding: UTF-8 -*-

""" crawler node """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import re
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle
import time
from DNS_Resolver import DNSResolver
import requests
from lxml import etree
from NetworkHandler import NetworkHandler
from unbuffered_output import uopen

left_ip = '127.0.0.1'
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
        #TODO:could check the Content-Type of the resp to make sure that
        #     it's not a image or video
        response = requests.get(resolved_url, headers=headers, timeout=60)
        print("Get response[%d]" % response.status_code)
        with lock:
            log.write(
                    "Response[%d] of url:[%s]\n" % (response.status_code, resolved_url))
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
                time.sleep(0.5)
                continue
            if not url_dict:
                print("Empty urls from dns_resolver. Crawler exit")
                log.write("Empty urls from dns_resolver. Crawler exit\n")
                break

            #将解析成功的和未成功的分开来
            resolved_dict = {}
            fail_resolved_dict = {}
            for key, value in url_dict.items():
                if value:
                    resolved_dict[key] = value
                else:
                    fail_resolved_dict[key] = None

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
                    try:
                        text = self.change_to_string(resp)
                    except Exception as e:
                        log.write("Fail to change_to_string for url:[%s]\n" % str(origin))
                        text = resp.text
                    outer_links, inner_links = self.extract_link(origin, text)
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
            match = re.match(pattern, element)
            if match:  # begin with http
                # test the header for spcific definition
                test_domain = re.match(domain_pattern, element).group()
                if test_domain != domain:
                    outer_link_lists.append(match.string)
                else:  # same domain
                    inner_link_lists.append(element)
            else:  # not begin with http
                # test if it's a css or javascript file
                test_inner = re.match(useless_pattern, element)
                if not test_inner:
                    test_tag = re.match(tag_pattern, element)
                    if test_tag:  # test if it's a page tag#
                        pass
                    else:
                        link = domain + '/' + element
                        inner_link_lists.append(link)
                else:
                    pass

        return (outer_link_lists, inner_link_lists)

if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()

