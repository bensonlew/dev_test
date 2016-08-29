# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import os
import argparse
import time
import multiprocessing
import psutil
import errno
from mbio.workflows.single import SingleWorkflow
from biocluster.wsheet import Sheet


def friendly_size(size):
    gb = 1024 * 1024 * 1024
    mb = 1024 * 1024
    kb = 1024
    if size >= gb:
        return "{:.2f}G".format(size / gb)
    elif size >= mb:
        return "{:.2f}M".format(size / mb)
    elif size >= kb:
        return "{:.2f}M".format(size / kb)
    else:
        return "{:2f}".format(size)


def get_res(main_p, targetPath):
    while True:
        child_p = main_p.children(recursive=True)
        for my_p in child_p:
            file_name = os.path.join(targetPath, str(my_p.pid) + "_res.txt")
            with open(file_name, 'ab') as a:
                cmd = " ".join(my_p.cmdline())
                cpu_percent = my_p.cpu_percent()
                memory_percent = my_p.memory_percent()
                memory_info = my_p.memory_info()
                memory_rss = friendly_size(memory_info[0])
                memory_vms = friendly_size(memory_info[1])
                a.write("pid:{}\tcpu_percent:{}\tmemory_percent:{}\tmemory_rss:{}\tmemory_vms:{}\tcmd:{}\n".format(my_p.pid, cpu_percent, memory_percent, memory_rss, memory_vms, cmd))
        time.sleep(5)

parser = argparse.ArgumentParser(description='统计一个Tool的资源占用情况')
parser.add_argument("-j", "--json", help="发起任务所需要的json", required=True)
parser.add_argument("-t", "--targetPath", help="资源统计文件的输出目录", required=True)
args = vars(parser.parse_args())

targetPath = os.path.abspath(args["targetPath"])

try:
    os.makedirs(targetPath)
except OSError as exc:
    if exc.errno != errno.EEXIST:
        raise exc
    else:
        pass

pid = os.getpid()
main_p = psutil.Process(pid)

wsheet = Sheet(args["json"])
wsheet.instant = True
wsheet.USE_DB = False
wf = SingleWorkflow(wsheet)
p2 = multiprocessing.Process(target=get_res, args=(main_p, targetPath))
p2.start()
try:
    wf.run()
except Exception as e:
    raise Exception(e)
p2.terminate()
