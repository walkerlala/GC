#coding:utf-8

""" helper class and function for unbuffered output """

class Unbuffered:
    """ helper class for uopen """
    def __init__(self, stream):
        self.stream = stream
    def write(self, data, *args, **kwargs):
        self.stream.write(data, *args, **kwargs)
        self.stream.flush()
    def __getattr__(self, attr):
        return getattr(self.stream, attr)

def uopen(filename, mode, buffering=None):
    """ make an unbuffer write, so every thing would be unbuffered,
        which means that you can see your output at once """
    fh = open(filename, mode)
    fh = Unbuffered(fh)
    return fh
