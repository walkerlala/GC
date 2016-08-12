from lxml import etree
# from io import StringIO
# from math import log2


def read_from_file(file_name):
    f = open(file_name, 'r')
    s = f.read()
    f.close()
    return s


class DomNode:
    """Class to represent a DOM tree node
       Give a root node and wo can get a DOM tree"""

    def __init__(self, element=etree.Element("html")):
        """Build a Dom node with a html label's
           tag name, attribute list, text content,
           number of children nodes, and children node list"""
        self._tag = element.tag
        self._attributes = element.attrib
        self._text = element.text
        self._length = len(element)
        self._children = []
        self._level = 0
        for index in range(self._length):
            self._children.append(DomNode(element[index]))

    def increment_level(self, parent_level):
        self._level = parent_level + 1

    def dfs_traverse(self):
        """DFS traverse from the root node"""
        try:
            print(self._level*" " + self._tag, end=" ")
        except TypeError:
            print(self._level*" " + "!!!!!!!!!!!!!!!!!!!!!!!!!Unknown tag!!!!!!!!!!!!!!!!!!!!!!!!!")

        # print(self._length, end=" ")
        if self._attributes:
            print(self._attributes, end=" ")
        if (self._text is not None) and (self._text.strip()):
            print(" [text:]" + self._text.strip(), end=" ")
        for index in self._children:
            print('')
            index.increment_level(self._level)
            index.dfs_traverse()


test_html = read_from_file("test.txt")

test = etree.HTML(test_html)

root = DomNode(test)

root.dfs_traverse()

# uncompleted bfs traverse
"""
def bfs_traverse(root_node):
    node_queue = []
    current_node = root_node
    node_queue.append(current_node)
    level = 0
    while node_queue:
        current_node = node_queue.pop(0)
        level += 1
        if log2(float(level))-int(log2(float(level))) < 1e-8:
"""

