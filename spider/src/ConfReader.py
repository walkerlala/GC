#!/usr/bin/python3
#coding: utf-8

import sys

""" configuration reading utility """

# pylint: disable=superfluous-parens, invalid-name, broad-except

class ConfReader(object):
    """ Read and return configuration.
        If the value of a configuration can be converted to int,
        then return an int, else a string.
        If the configuration required is not presented in the configuration
        file, then a None would be returned """

    def __init__(self, conf_file):
        self.conf_dict = {}
        with open("conf/" + conf_file, "r") as f:
            for line in f:
                if not line.strip() or line.strip().startswith('#'):
                    continue
                parts = line.split('#', 1)[0].strip().split("=")
                if len(parts) != 2:
                    print("Unrecognized Configuration: [%s]\n" % line, file=sys.stderr)
                    continue
                parts = list(map(lambda x:x.strip(), parts))
                try:
                    second = int(parts[1])
                except Exception:
                    second = parts[1]
                self.conf_dict[parts[0]]=second

    def get(self, name):
        if name in self.conf_dict:
            return self.conf_dict.get(name)
        else:
            return None

