#!/usr/bin/python3

# 分词之后（所有词语都用,分隔了），从tainingDB里面讲所有词语提取出来，
# 然后排序标号，以便用svm分类。注意因为要区分 title，keywords,
# 和 description， 所以三者的标号有所区别
#

#### user configuration ######
hostname="localhost"
username="root"
password="123456"
charset="utf8"

trainingDB_name="NEW_trainingDB"
training_table_name="pages_table"

crawlerDB_name="NEW_crawlerDB"
crawler_table_name="pages_table"

training_features_file="trainingDB.features"
crawler_features_file="crawler.features"

from MySQLdb import *
import re
import os

### util function
def unique(sequence):
    """ generate unique item from sequence in the order of first occurrence"""
    seen=set()
    for value in sequence:
        if value in seen:
            continue
        seen.add(value)
        yield value
### end util function ###

####### doing `trainingDB' ######
db=connect(
        host = hostname,
        user = username,
        passwd = password,
        db   = trainingDB_name,
        charset = charset)

cur=db.cursor();

training_features_file_handle=open(training_features_file,"w+")

cur.execute("select `title`,`keywords`,`description` from " + training_table_name);
training_features=[]
for title, keywords, description in cur.fetchall():
    training_features.extend(title.split(",")).extend(keywords.split(",")).extend(description.split(","))
training_features=unique(sorted(training_features))
for number, feature in enumerate(training_features):
    training_features_file_handle.write(str(number) + ": " + feature+"\n")

#### doing `crawlerDB' ####
"""
db=connect(
        host = hostname,
        user = username,
        passwd = password,
        db   = crawlerDB_name,
        charset = charset)

cur=db.cursor();

crawler_features_file_handle = open(crawler_features_file, "w+")

cur.execute("select `title`, `keywords`, `description` from " + crawler_table_name)
crawler_features=[]
for title, keywords, description in cur.fetchall():
    crawler_features.extend(title.split(",")).extend(keywords.split(",")).extend(description.split(","))
crawler_features=unique(sorted(crawler_features))
for feature in crawler_features:
    try:
        index=training_features.index(feature)
        crawler_features_file_handle.write(str(number) + ": " + feature+"\n")
"""


