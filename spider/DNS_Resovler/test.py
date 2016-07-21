from DNS_Resolver import *

resolver = DNSResolver()

resolver.get_resolved_url_packet()


for k, v in DNS_cache.get_cache().items():
    print(k, ";", v)

resolver.print_buffer()
