#!/usr/bin/python3
#coding:utf-8

import Bloom_filter
from unbuffered_output import uopen

bf = Bloom_filter.Bloom_filter(10000, 0.001, filename=("/tmp/blmt",-1), start_fresh=True)

num = 100

for i in range(1000):
    string = str(num + i)
    bf.add(string)

count = 0
for i in range(1000):
    string = str(num + i)
    if string in bf:
        count = count + 1
print("Num in bf is %d\n" % count)

hit = 0
for i in range(1000):
    string = str(num + i)
    if string not in bf:
        hit = hit + 1
print("Num not in bf is %d\n" % hit)

