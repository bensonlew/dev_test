# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import argparse
import os


def get_size(path):
    """
    获取文件大小
    """
    b = os.path.getsize(path)
    if b / 1024 < 1:
        return "{}B".format(b)
    elif b >= 1024 and b < 1024 * 1024:
        return "{:.3f}KB".format(b / 1024)
    elif b >= 1024 * 1024 and b < 1024 * 1024 * 1024:
        return "{:.3f}MB".format(b / (1024 * 1024))
    else:
        return "{:.3f}GB".format(b / (1024 * 1024 * 1024))


parser = argparse.ArgumentParser(description="根据提供的路径, 解析该路径的目录结构")
parser.add_argument("-i", "--input", help="输入的文件的绝对路径", required=True)
parser.add_argument("-o", "--output", help="", required=True)
args = vars(parser.parse_args())

target_path = os.path.abspath(args["output"])
source_path = args["input"]
file_list = list()

if not os.path.isabs(source_path):
    raise Exception("{} 不是一个绝对路径".format(source_path))

if not os.path.isdir(source_path):
    raise Exception("{} 文件夹不存在或者不是一个合法的文件夹".format(source_path))

try:
    with open(target_path, "wb") as w:
        w.write("#source#{}\n".format(source_path))
        w.write("{}\t{}\t{}\t{}\n".format("文件路径", "文件大小", "文件描述", "是否锁定"))
except Exception as e:
    raise Exception(e)

for d in os.walk(source_path):
    for f in d[2]:
        full_path = os.path.join(d[0], f)
        file_size = get_size(full_path)
        file_list.append([full_path, file_size])

with open(target_path, "ab") as a:
    for l in file_list:
        a.write("{}\t{}\t{}\t{}\n".format(l[0], l[1], "", "True"))
