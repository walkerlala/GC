import collections

# use Singleton pattern
def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

@singleton
class DNSCache(object):
    """class to store local DNS cache
    """

    def __init__(self):
        self._cache = collections.OrderedDict()
        self._size = 10000

    # set cache size
    def set_size(self, size):
        self._size = size

    # add new host-ip pair
    def add_to_cache(self, new_host, new_ip):
        if len(self._cache) < self._size:
            self._cache[new_host] = new_ip
        else:
            self._cache.popitem(last=False)
            self._cache[new_host] = new_ip

    # query a host-ip pair, return the result of query
    def query_cache(self, host, ip):
        if host in self._cache:
            if self._cache[host] == ip:
                return True
            else:
                return False
        else:
            return False

    # return ip according to supplied host
    def wanted_ip(self, host):
        return self._cache[host]

    # return DNS cache table
    def get_cache(self):
        return self._cache

