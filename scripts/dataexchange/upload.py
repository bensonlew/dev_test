#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import argparse
import os
from upload_task import UploadTask


parser = argparse.ArgumentParser(description="根据验证码，上传一个文件夹至某一个项目下")
parser.add_argument("-c", "--identity_code", help="验证码", required=True)
parser.add_argument("-l", "--file_list", help="文件列表, 用于描述文件的信息, 以及是否允许让客户直接下载", required=True)
parser.add_argument("-s", "--silence", help="静默模式，当把该值设为True时, 将不再在屏幕上输出日志信息，默认为False", default="False")
parser.add_argument("-m", "--mode", help="模式, 为sanger或者tsanger中的一个， 默认为sanger", default="sanger")
parser.add_argument("-p", "--port", help="端口号，通常为80或者2333，默认为80", default="80")
args = vars(parser.parse_args())

if not os.path.exists(args["file_list"]):
    raise OSError("列表文件 {} 不存在".format(args["file_list"]))

if args["silence"] not in ["True", "False"]:
    raise ValueError("参数-s的值必须是True或者是False")

if args["mode"] not in ["sanger", "tsanger"]:
    raise ValueError("参数-m的值必须是sanger或者是tsanger")

if args["silence"] == "False":
    stream_on = True
else:
    stream_on = False

task = UploadTask(args["identity_code"], os.path.dirname(args["file_list"]), args["mode"], args["port"], stream_on)
info = task.get_task_info()
file_info = task.get_file_info(args["file_list"])
task.upload_files()
