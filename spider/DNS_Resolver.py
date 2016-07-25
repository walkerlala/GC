import socket
import urllib.parse
from DNS_Cache import *
from NetworkHandler import NetworkHandler
import threading

socket.setdefaulttimeout(60)

class DNSResolver:
    """ Class to help resolve url's IP address """

    DNS_cache = DNSCache()
    log_file = open("DNSResolver.log","w+")
    lock = threading.Lock()

    def __init__(self, ip, port):
        #_source_ip, _source_port即links的来源
        self._source_ip = ip
        self._source_port = port
        self.links_requester = NetworkHandler(self._source_ip, self._source_port)

        self._url = ""
        self._url_suffix = ""
        self._host = ""
        self._ip = ""
        self.protocol=""
        self._buffer = collections.OrderedDict()
        self._buffer_size = 200
        self._buffer_size_threshold=30
        self._nsendlinks = self._buffer_size_threshold

    # set url
    def set_url(self, url):
        self._url = url

    # get domain name and url's suffix
    def resolve_host(self, url):
        self.set_url(url)
        # remove protocol
        self.protocol, rest = urllib.parse.splittype(self._url)
        # split domain name and url's suffix
        # TODO: need to check whether this method is sufficient to split those
        #       complicated url into host and suffix
        self._host, self._url_suffix = urllib.parse.splithost(rest)
        with DNSResolver.lock:
            DNSResolver.log_file.write("host[%s], url_suffix[%s]\n" %
                                        (str(self._host),str(self._url_suffix)))

    # get ip address
    def resolve_ip(self):
        #TODO: there should be some try, execept
        links = self.links_requester.request()
        print("DNS_Resolver successfully get some links:[%s]\n" % str(links))
        for url in links:
            url = url.strip()
            self.resolve_host(url)
            if DNSResolver.DNS_cache.query_cache(self._host, self._ip):
                self._ip = DNSResolver.DNS_cache.wanted_ip(self._host)
            else:
                try:
                    print("No url-ip pair found. querying DNS server...")
                    self._ip = socket.gethostbyname(self._host)
                    print("Successfully get ip[%s] from DNS server\n" % str(self._ip))
                    DNSResolver.DNS_cache.add_to_cache(self._host, self._ip)
                except socket.gaierror:
                    self._ip = ""
                except Exception as e:
                    self._ip = ""
                    # print("cannot attach the remote server")
            self._buffer[self._url] = self.protocol + "://" + self.get_resolved_url()
            self._buffer_size = len(self._buffer)

    # return primitive url
    def get_url(self):
        return self._url

    # return resolved url's suffix
    def get_url_suffix(self):
        return self._url_suffix

    # return resolved domain name
    def get_host(self):
        return self._host

    # return resolved domain name
    def get_ip(self):
        return self._ip

    # return resolved url
    def get_resolved_url(self):
        if self._ip == "":
            return None
        else:
            return self._ip + self._url_suffix

    # accept page_fetcher's request and response
    def get_resolved_url_packet(self):
        temp = []
        #while (len(self._buffer) < self._buffer_size_threshold):
            #when _buffer is pretty small, get some links
        self.resolve_ip()   #resolve a bunch of links to ip_url

        #fullfill temp and return it to the caller
        if self._buffer:
            for _ in range(self._nsendlinks):
                temp.append(self._buffer.popitem(last=False))
        return dict(temp)

