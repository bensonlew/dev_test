# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import argparse
import os
import errno
from download_task import DownloadTask


parser = argparse.ArgumentParser(description='根据验证码，下载一个任务的任务文件')
parser.add_argument("-c", "--identityCode", help="验证码", required=True)
parser.add_argument("-t", "--targetPath", help="目标路径，用于指定将任务文件下载到该目录下，请保证你对该目录有写权限", required=True)
parser.add_argument("-s", "--silence", help="静默模式，当把该值设为True时, 将不再在屏幕上输出日志信息，默认为False", default="True")
parser.add_argument("-m", "--mode", help="模式, 为sanger或者tsanger中的一个， 默认为sanger", default="sanger")
parser.add_argument("-p", "--port", help="端口号，通常为80或者2333，默认为80", default="80")
args = vars(parser.parse_args())

targetPath = os.path.abspath(args["targetPath"])

try:
    os.makedirs(targetPath)
except OSError as exc:
    if exc.errno != errno.EEXIST:
        raise exc
    else:
        pass

if args["silence"] not in ["True", "False"]:
    raise ValueError("参数-s的值必须是True或者是False")

if args["mode"] not in ["sanger", "tsanger"]:
    raise ValueError("参数-m的值必须是sanger或者是tsanger")

if args["silence"] == "True":
    stream_on = True
else:
    stream_on = False

task = DownloadTask(args["identityCode"], args["targetPath"], args["mode"], args["port"], stream_on)
task.get_task_info()
task.download_files()
