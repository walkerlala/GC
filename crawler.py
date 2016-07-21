# -*- coding: UTF-8 -*-
from DNS_Resolver import DNSReslover
import requests
from lxml import etree
import re
import Queue
import threading


class Crawler(threading.Thread):
    """Class for crawler.
       It can grab the web information and extract the inner and the outer links
          and store the whole html file in database.
       You can call the run_crawler() function to run the crawler.
       Just use the attributes outer_link_set and inner_link_set getting link."""
    q = Queue.Queue()
    dic = {}

    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self.text = ""
        self.url = ""

    def run(self):
        """This is  for run a crawler."""
        while True :
            if Crawler.q.empty():
                self.get_url()
                continue
            print "Grab by " + self.getName()
            self.url = Crawler.q.get()
            response = self.get_web()
            while not response:
                self.url = Crawler.q.get()
                response = self.get_web()
            text = self.change_to_string(response)
            result = self.extract_link()
            outer_link = []
            inner_link = []
            self.handle_link( result, inner_link, outer_link)
            outer_link_set = set(outer_link)
            inner_link_set = set(inner_link)
            Crawler.dic[self.url] = inner_link_set

    def get_inner_links(self):
        """The interface for inner links"""
        return Crawler.dic

    def get_url(self):
        """This function is used for get url list from the left"""
        urls_dic = resolver.get_resolved_url_packet()
        for url in urls_dic.keys():
            if not urls_dic[url]:
                Crawler.dic[url] = 'fail'
            Crawler.q.put(urls_dic[url])

    def get_web(self):
        """This function used for grab a web information and return a Response object."""
        headers = {'User-Agent': 'XXX', 'Referer': 'XXX'}
        proxies = {'http': 'http://XX.XX.XX.XX:XXXX'}
        try:
            responses = requests.get(self.url)
            return responses
        except Exception:
            Crawler.dic[self.url] = "fail"
            return False

    def change_to_string(self, response):
        """Change the Response object to string and return it"""
        html_text = etree.HTML(response.text)
        self.text = etree.tostring(html_text, pretty_print=True)

        domain_pattern = re.compile(r'http://[\w+\.]+|https://[\w+\.]+')
        http_domain = re.match(domain_pattern, self.url).group()
        split_pattern = re.compile('http://')
        domain_list = re.split(split_pattern, http_domain)
        change_pattern = re.compile('\.')
        domain = re.sub(change_pattern, '_', domain_list[1])

        name = domain + '.txt'
        #self.store_text(name)
        # print texts

    def store_text(self, name):
        """This function is used for get the text of the web."""
        f = file(name, 'a+')
        f.write(self.text)
        f.close()

    def extract_link(self):
        """This function is used for extract all links from the web.
           Use the href attributes, Return the result."""
        html_text = etree.HTML(self.text)
        results = html_text.xpath('//@href')
        return results

    def handle_link(self, results, inner_link_lists, outer_link_lists):
        """The function is used for deal with the links.
           Distinct the inner links and outer links.
           For inner links, it should add the header and delete the tag#, remove .css and javascript link"""
        # distinct inner from outer link through the header http
        pattern = re.compile(r'http://|https://')
        # get the url domain to define the website
        domain_pattern = re.compile(r'http://[\w+\.]+|https://[\w+\.]+')
        domain = re.match(domain_pattern, self.url).group()
        # define the .css or javascript file
        useless_pattern = re.compile(r'/|javascript|\S*.css')
        # define the tag#
        tag_pattern = re.compile(r'\S*#\S*')
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
                    test_tag = re.match(tag_pattern,element)
                    if test_tag:  # test if it's a page tag#
                        pass
                    else:
                        link = domain + '/' + element
                        inner_link_lists.append(link)
                else:
                    pass


def run_crawler():
    thread1 = Crawler("thread1")
    thread2 = Crawler("thread2")
    thread3 = Crawler("thread3")
    thread2.start()
    thread1.start()
    thread3.start()
    thread2.join()
    thread3.join()
    thread1.join()

resolver = DNSReslover()
run_crawler()








