#!/usr/bin/python3
#coding:utf-8

import Bloom_filter
import string
import random

b = Bloom_filter.Bloom_filter(10000, 0.001, filename=("/tmp/blmt",-1), start_fresh=True)

sl = []
for _ in range(500000):
    chars = ''.join(random.choice(string.ascii_letters + string.digits + "%_-/?#$!()[]*") for i in range(120))
    sl.append(chars)

print("Generate random string completed")

for s in sl:
    b.add(s)

print("Added all to bloom filter")

wrong_count = 0
for s in sl:
    if s not in b:
        wrong_count += 1
        print("wrong-count: %d " % wrong_count)

print("test completed. wrong_count: %d " % wrong_count)
