from DOM_Tree import *


class StyleNode:
    """Class to represent a StyleNode in a style tree"""

    def __init__(self, element=DomNode()):
        self._count = 1  # the number of pages has this style tree
        self._nodelist = []
        for index in range(element.length()):
            self._nodelist.append(ElementNode(element[index]))
        self._level = 0

    def increment_level(self, parent_level):
        self._level = parent_level + 1

    def print_self(self, parent_level):
        self.increment_level(parent_level)
        for index in self._nodelist:
            index.set_level(self._level)
            index.print_self()


class ElementNode:
    """Class to represent a ElementNode in a style tree"""

    def __init__(self, element=DomNode()):
        self._length = 1  # the number of style nodes below E
        self._tag = element.tag()
        self._attributes = element.attributes()
        self._text = element.text()
        self._children = []
        child = StyleNode(element)
        self._children.append(child)
        self._level = 0

    def increment_level(self, parent_level):
        self._level = parent_level + 1

    def set_level(self, temp):
        self._level = temp

    def print_self(self):
        if self._tag is not None:
            print(self._level * " " + "<" + self._tag + ">", end=" ")
        # if self._attributes:
            # print(self._attributes, end="  ")
        if self._text is not None:
            print("[text]:" + self._text.strip(), end="  ")
        # print("[length]:", self._length, end=" ")
        # print("</" + self._tag + ">", end=" ")
        for index in self._children:
            print('')
            index.increment_level(self._level)
            index.print_self(self._level)
        # print("</" + self._tag + ">", end=" ")


def dom_to_sst(html):
    """transfer a DOM tree into a style tree"""

    html = etree.HTML(html)
    root = dom_tree(html)
    root = ElementNode(root)
    root.print_self()


test_html = read_from_file("test1.txt")
dom_to_sst(test_html)
