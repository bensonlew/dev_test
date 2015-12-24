# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# __version__ = 'v1.0'
# __last_modified__ = '20151222'
import os
import re
from biocluster.config import Config


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


def code2index(code):
    """
    根据一个index的代码，获取具体的index序列
    """
    flag = 1
    database = os.path.join(Config().SOFTWARE_DIR, "datasplit/barcode.list")
    with open(database, 'r') as r:
        for line in r:
            line = line.rstrip('\n')
            line = re.split('\t', line)
            if line[0] == code:
                varbase = line[1]
                left_index = line[2]
                right_index = line[3]
                flag = 0
                break
    if flag:
        raise ValueError("未找到该index代码")
    if len(varbase) == 1:
        f_varbase = varbase
        r_varbase = varbase
    elif len(varbase) == 2:
        f_varbase = varbase[0]
        r_varbase = varbase[1]
    return (left_index, right_index, f_varbase, r_varbase)


def code2primer(code):
    """
    根据一个primer的代码，获取具体的primer
    """
    database = os.path.join(Config().SOFTWARE_DIR, "datasplit/primer.list")
    code = re.split('_', code)
    f_primer = ""
    r_primer = ""
    with open(database, 'r') as r:
        for line in r:
            line = line.rstrip('\n')
            line = re.split('\t', line)
            if line[0] == code[0]:
                f_primer = line[1]
            if line[0] == code[1]:
                r_primer = line[1]
    if f_primer == "" or r_primer == "":
        raise ValueError("未找到该primer代码")
    return (f_primer, r_primer)


def str_check(real_str, list_str):
    """
    比较两个index，返回两个字符串不同字符的个数
    """
    length = len(real_str)
    count = 0
    ABBR = {"A": "AA", 'C': "CC", 'T': "TT", 'G': "GG",
            'M': "AC", 'R': "AG", 'W': "AT", 'S': "CG",
            'Y': "CT", 'K': "GT", 'V': "ACG", 'H': "ACT",
            'D': "AGT", 'B': "CGT", 'X': "ACGT", 'N': "ACGT"}
    for i in range(length):
        realbase = real_str[i]
        if list_str[i] in ABBR.keys():
            indexbase = ABBR[list_str[i]]
        else:
            indexbase = list_str[i]
        if not re.search(realbase, indexbase):
            count = count + 1
    return count
