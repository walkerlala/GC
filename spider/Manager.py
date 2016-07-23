#!/usr/bin/python3
#encoding:utf-8

import socket
import os
import datetime
import sys
import pickle
import threading

socket.setdefaulttimeout(60)

class PriQueue:
    """ a priority queue that used to stored links
        and dispatch links to the right """
    def __init__:
        self.buf = []
        self.links = []

    def get_links_from_disk(self):
        """ This is test method. Use this to get links
            from disk, as we have not implement the 
            prio method """
        pass

    def get(self):
        """ get links according to prio of links """
        pass
    def append(self):
        """ feed links in to the internal prio queue,
            note that those links are ordered by prio """
        pass
    
class Manager:
    "Core Manager ""
    
    def __init__(self, ip, port, buff_size=1024, listen_num=5, thread_num=10):
        self.ip = ip
        self.port = port
        self.buff_size = buff_size
        self.listen_num = listen_num
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.ip, self.port))
        sock.listen(self.listen_num)
        #TODO: initialize bloom filter
        self.bloom_filter = None
        self._lock = threading.Lock() #lock to access bloom_filter
        self.thread_list = []
        self.thread_num = thread_num
        self._log = open("Manager.log","w+")
        self.SEND = 1
        self.REQUEST = 0

    def handle_connection(self, conn):
        establish = False
        method = None
        data_buf = []
        try:
            method = self.get_conn_type(conn)
            if (method == self.SEND):#client want to send links
                while True:
                    data = conn.recv(self.buff_size)
                    if data:
                        data_buf.append(data)
                    else:
                        break
                #TODO send this thing to bloom map after priority queue after unpickle and filter
            elif (method == self.REQUEST):
                #TODO send it some links
                pass

                
    def get_conn_type(self, conn):
        while True:
            try:
                data = conn.recv(self.buff_size)
                if (data == b'SEND'):
                    return self.SEND
                elif (data == b'REQUEST'):
                    return self.REQUEST
                else:
                    return None
            except Exception as e:
                raise

    def run(self):
        self._log.write("Manager start running at: %s" %
                              datetime.datetime.now().strftime("%B %d, %Y")
        while(True):
            #only want a fix number of thread in this program
            if (len(self.thread_list) > self.thread_num):
                for thread in self.thread_list:
                    thread.join()
                self.thread_list = []
            conn, addr = self.sock.accept()
            self._log.write("Connection established: %s\n" % str(addr))
            t = threading.Thread(target=self.handle_connection)
            self.thread_list.append(t)
            t.start()


