# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# __version__ = 'v1.0'
# __last_modified__ = '20151222'


def reverse_complement(string):
    """
    将一个序列进行互补
    :param string: 输入的序列
    """
    length = len(string)
    newstring = ""
    for i in range(length):
        if string[i] == "A":
            newstring = newstring + "T"
        elif string[i] == "a":
            newstring = newstring + "t"
        elif string[i] == "T":
            newstring = newstring + "A"
        elif string[i] == "t":
            newstring = newstring + "a"
        elif string[i] == "C":
            newstring = newstring + "G"
        elif string[i] == "G":
            newstring = newstring + "C"
        elif string[i] == "c":
            newstring = newstring + "G"
        elif string[i] == "g":
            newstring = newstring + "c"
        else:
            newstring = newstring + string[i]
        return newstring
