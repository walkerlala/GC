#!/usr/bin/python3
#coding:utf-8

"""
    CAVEAT HERE !
    we don't need a crawler level DNS-Resolver anymore !!!
    To dns-caching, use a program called `dnsmasq'. On Debian/ubuntu, just
    install `dnsmasq' and `network-manager'. dnsmasq would change
    /etc/resolv.conf and set "nameserver 127.0.1.1"(or "nameserver
    127.0.0.1") and then it would listen on that loop interface. When your
    application make a DNS query, it would query the server at 127.0.1.1(as
    indicated by /etc/resolv.conf), which would by passed to `dnsmasq', which
    would do all the necessary dns query and caching...

"""

""" DNSResolver is reponsible for resolving url=>ip.
    Also, it's responsible for reqeusting links from Manager """

# pylint: disable=superfluous-parens, invalid-name, broad-except

import socket
import urllib.parse
import threading
import mmap
from NetworkHandler import NetworkHandler
from unbuffered_output import uopen

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
                protocol, rest = urllib.parse.splittype(link)
                host, url_suffix = urllib.parse.splithost(rest)
                resolved = None
                for dictionary in DNSResolver.DNS_cache:
                    if host == list(dictionary.keys())[0]:
                        ip_address = list(dictionary.values())[0]
                        resolved = protocol + "://" + ip_address + url_suffix
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
        """ used to get url=>resolved_url from Manager side.
            Note that we don't have to set any timeout here,
            because, after all, crawler have to get some links from
            Manager side before it can continue"""
        if len(self._buffer) < self._buffer_size_threshold:
            try:
                links = self.links_requester.request()
                if not links:
                    #return whatever in self._buffer
                    tmp = self._buffer
                    self._buffer = {}
                    return tmp
                else:
                    #resolved_links = self.resolve_url(links)
                    #self._buffer.update(zip(links, resolved_links))
                    self._buffer.update(zip(links, links))
                    #make sure that we don't exceed the limit
                    nsent = (self._nsent if self._nsent <= len(self._buffer)
                                            else len(self._buffer))
                    tmp = {}
                    pairs = list(self._buffer.items())
                    to_be_del = []
                    for index in range(nsent):
                        url, resolved = pairs[index]
                        tmp[url] = resolved
                        to_be_del.append(url)
                    for item in to_be_del:
                        del self._buffer[item]
                    return tmp
            except Exception:
                raise
        else:
            #make sure that we don't exceed the limit
            nsent = (self._nsent if self._nsent <= len(self._buffer)
                                    else len(self._buffer))
            #we have enough links, so just return
            tmp = {}
            pairs = list(self._buffer.items())
            to_be_del = []
            for index in range(nsent):
                url, resolved = pairs[index]
                tmp[url] = resolved
                to_be_del.append(url)
            for item in to_be_del:
                del self._buffer[item]
            return tmp
