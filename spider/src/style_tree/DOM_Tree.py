from lxml import etree


def read_from_file(file_name):
    """Open a test file and return a html string"""

    f = open(file_name, 'r')
    s = f.read()
    f.close()
    return s


def dom_tree(html_tree):
    """Return a DOM tree with the root node <body> """

    for index in range(len(html_tree)):
        if html_tree[index].tag == 'body':
            body = DomNode(html_tree[index])
            # body.dfs_traverse()
            return body


class DomNode:
    """Class to represent a DOM tree
       Give a root node and wo can get a DOM tree"""

    def __init__(self, element=etree.Element("html")):
        """Build a DOM node with a html label's
           tag name, attribute list, text content,
           number of children nodes, and children node list"""

        self._length = len(element)
        if isinstance(element.tag, str):
            self._tag = element.tag
        else:
            self._tag = None
        self._attributes = element.attrib
        if (element.text is not None) and (element.text.strip()):
            self._text = element.text
        else:
            self._text = None
        self._children = []
        for index in range(self._length):
            if isinstance(element[index].tag, str):
                self._children.append(DomNode(element[index]))
        self._length = len(self._children)
        self._level = 0

    def increment_level(self, parent_level):
        self._level = parent_level + 1

    def dfs_traverse(self):
        """DFS traverse from the root node"""
        if self._tag is not None:
            print(self._level*" " + "<" + self._tag + ">", end=" ")
        if self._attributes:
            print(self._attributes, end=" ")
        if self._text is not None:
            print(" [text]:" + self._text.strip(), end=" ")
        print("[length]:", self._length, end=" ")
        for index in self._children:
            print('')
            index.increment_level(self._level)
            index.dfs_traverse()
        print(" </" + self._tag + ">", end=" ")

    def tag(self):
        return self._tag

    def attributes(self):
        return self._attributes

    def text(self):
        return self._text

    def children(self):
        return self._children

    def length(self):
        return self._length

    def __getitem__(self, item):
        return self._children[item]

# test_html = read_from_file("test3.txt")

# test = etree.HTML(test_html)

# dom_tree(test)
