#!/usr/bin/python3
#coding:utf-8
from multiprocessing.dummy import Pool as ThreadPool
import urllib2
urls = [ "http://www.xxxasdfasdf.com",
        "http://www.sdasdgertasdfasdgasd.com"
        ]
results = map(urllib2.urlopen, urls)
for r in results:
    print(r)
