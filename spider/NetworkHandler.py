#!/usr/bin/python
#coding:utf-8

""" Networking transfering utility """

import socket
import threading
import pickle
from unbuffered_output import uopen #unbuffered open

class NetworkHandler:
    """ handle network
        NOTE that we haven't set any timeout value for any connection here,
        because NetworkHandler is used by crawler side and doing so can avoid
        crawler side closing the connection before Manager side have time to
        send any data"""

    log = uopen("NetworkHandler.log", "w+")
    log_lock = threading.Lock() #lock to access log

    def __init__(self, ip, port):
        self._ip = ip
        self._port = port

    #data have to be raw(byte) string, not unicode
    def send(self, data):
        """ send data to (self._ip, self._port) """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._ip, self._port))
            #与worker建立连接，表示要发送数据
            if self.establish_context(sock, b"SEND"):
                sock.sendall(data)
                sock.close()
            else:
                with NetworkHandler.log_lock:
                    NetworkHandler.log.write(("Cannot establish valid connection(SEND)"
                        "with worker/server[%s,%s]\n") % (str(self._ip), str(self._port)))
                raise Exception(("Cannot establish valid connection(SEND) with"
                    "worker/server[%s,%s]") % (str(self._ip), str(self._port)))
        except Exception:
            raise
    def request(self):
        """ request data from (self._ip, self._port) """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self._ip, self._port))
            if self.establish_context(sock, b"REQUEST"):
                links_raw = []
                while True:
                    try:
                        data = sock.recv(512)
                        if not data:  #finish getting data
                            break
                        links_raw.append(data)
                    except Exception:
                        #既然中断了连接，数据肯定不完整
                        raise
                links = b''.join(links_raw)
                print("NetworkHandler get data:[%s]\n" % str(links))
                sock.close()
                return pickle.loads(links)
            else:
                with NetworkHandler.log_lock:
                    NetworkHandler.log.write(("Cannot establish valid connection(REQUEST)"
                        "with worker/server[%s,%s]\n") % (str(self._ip), str(self._port)))
                raise Exception(("Cannot establish valid connection(REQUEST) with"
                    "worker/server[%s,%s]") % (str(self._ip), str(self._port)))
        except Exception:
            raise

    def establish_context(self, sock, connect_type):
        """ establish proper connection with the opposite """
        try:
            sock.sendall(connect_type)
            resp = sock.recv(2)  #end of this round
            if (resp == b'OK'): #handshake
                return True
        except Exception:
            return False
        return False

