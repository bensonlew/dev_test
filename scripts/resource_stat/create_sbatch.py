# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import os
import argparse
import subprocess
import errno
from mako.template import Template


parser = argparse.ArgumentParser(description='生成一个投递脚本， 统计一个Tool的资源占用情况')
parser.add_argument("-j", "--json", help="发起任务所需要的json", required=True)
parser.add_argument("-t", "--targetPath", help="资源统计文件的输出目录", required=True)
parser.add_argument("-c", "--cpu", help="所申请的cpu数目, 整数, 值应当小于32", required=True)
parser.add_argument("-m", "--mem", help="所申请的内存的大小, 整数，单位G, 值应当小于250", required=True)
parser.add_argument("-n", "--name", help="投递出去任务的名称", required=True)
args = vars(parser.parse_args())
dic = dict()
dic["home"] = os.path.expanduser("~")
dic["cpu"] = args["cpu"]
dic["targetPath"] = args["targetPath"]
dic["name"] = args["name"]
dic["mem"] = args["mem"]
dic["json"] = args["json"]

try:
    os.makedirs(dic["targetPath"])
except OSError as exc:
    if exc.errno != errno.EEXIST:
        raise exc
    else:
        pass

if int(dic["cpu"]) > 32:
    dic["cpu"] = 32
else:
    dic["cpu"] = int(dic["cpu"])
if int(dic["mem"]) > 250:
    dic["mem"] = 250
else:
    dic["mem"] = int(dic["mem"])

tpl_path = os.path.join(dic["home"], "biocluster/scripts/resource_stat/res_stat.tpl")
sbatch_path = os.path.join(dic["targetPath"], dic["name"] + ".sbatch")
mytemplate = Template(filename=tpl_path)
string = mytemplate.render(dic=dic)
with open(sbatch_path, "wb") as w:
    w.write(string)
try:
    subprocess.check_output(["sbatch", sbatch_path])
except subprocess.CalledProcessError as e:
    print e
