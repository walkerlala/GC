#!/usr/bin/python3 -u
#coding:utf-8

""" DNSResolver is reponsible for resolving url=>ip.
    Also, it's responsible for reqeusting links from Manager """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import socket
import urllib.parse
import threading
import mmap
from NetworkHandler import NetworkHandler
from unbuffered_output import uopen

socket.setdefaulttimeout(60)

class DNSResolver:
    """ Class to help resolve url's IP address """
    #mmap_file = uopen("/tmp/DNSResolver.mmap","w+")
    #mmap_file.truncate(5*1024*1024) #500M space for the file
    #_mmap = mmap.mmap(mmap_file.fileno(),0) #0 means that whole file
    #DNS_cache = _mmap   #use _mmap as the same way as list in python
    DNS_cache = []   #we are not using memory mapping here for simplicity
    cache_lock = threading.Lock()  #lock to access DNS_cache
    log_file = uopen("DNSResolver.log", "w+")
    log_lock = threading.Lock()

    def __init__(self, ip, port):
        """ @_source_ip     : ip of the manager
            @_source_port   : port of the manager
            @links_requester: help instance that can help us get links from manager
            @_buffer        : buffer that hold some {url=>resolved_url...}
            @_buffer_size_threshold: if size of buffer is less than this, then we should get some
                                     from manager
            @_nsent         : number of links that we return at a time """
        self._source_ip = ip
        self._source_port = port
        self.links_requester = NetworkHandler(self._source_ip, self._source_port)
        self._buffer = {} #like {url1=>ip1, url2=>ip2, url3=>ip3...}
        self._buffer_size_threshold = 10
        self._nsent = self._buffer_size_threshold #how many links we should return when the caller
                                                  #call self.get_resolved_url_packet()

    def resolve_url(self, links):
        """ resolved the `links` passed in """
        resolved_links = []
        with DNSResolver.cache_lock:
            for link in links:
                #TODO: doubt this method
                protocol, rest = urllib.parse.splittype(link)
                host, url_suffix = urllib.parse.splithost(rest)
                resolved = None
                for dictionary in DNSResolver.DNS_cache:
                    if (host == list(dictionary.keys())[0]):
                        ip_address = list(dictionary.values())[0]
                        resolved = protocol + "://" + ip_address
                        break
                if not resolved:
                    try:
                        print("Querying DNS server...")
                        ip_address = socket.gethostbyname(host)
                        print("Get ip[%s] from DNS server" % str(ip_address))
                        DNSResolver.DNS_cache.append({host:ip_address})
                        resolved = protocol + "://" + ip_address + url_suffix
                    except Exception as e:
                        with DNSResolver.log_lock:
                            DNSResolver.log_file.write(
                                "Exception when querying DNS server:[%s]\n" % str(e))
                        resolved = None
                resolved_links.append(resolved)
        return resolved_links

    def get_resolved_url_packet(self):
        """ get a bunch of `url=>resolved_url` """
        if len(self._buffer) < self._buffer_size_threshold:
            try:
                links = self.links_requester.request()
                #if manager no longer have any links,
                #then we should use what we left. If no
                #more links can be given out, then we should
                #notify the caller by returning a empty dict
                if not links:
                    #if we still have some, just return it.
                    #Otherwise, we would return a empty dict
                    tmp = self._buffer
                    self._buffer = {}
                    return tmp
                else:
                    resolved_links = self.resolve_url(links)
                    self._buffer.update(zip(links, resolved_links))
                    #make sure that we don't exceed the limit
                    if (self._nsent < len(self._buffer)):
                        self._nsent = len(self._buffer)
                    #return self._nsent links
                    tmp = {}
                    iterator = iter(self._buffer)
                    to_be_del = []
                    for count, item in enumerate(iterator):
                        if (count < self._nsent):
                            tmp[item] = self._buffer[item]
                            to_be_del.append(item)
                    for item in to_be_del:
                        del self._buffer[item]
                    return tmp
            except Exception:
                raise
        else:
            #make sure that we don't exceed the limit
            if (self._nsent < len(self._buffer)):
                self._nsent = len(self._buffer)
            #we have enough links, so just return
            tmp = {}
            iterator = iter(self._buffer)
            to_be_del = []
            for count, item in enumerate(iterator):
                if (count < self._nsent):
                    tmp[item] = self._buffer[item]
                    to_be_del.append(item)
            for item in to_be_del:
                del self._buffer[item]
            return tmp

