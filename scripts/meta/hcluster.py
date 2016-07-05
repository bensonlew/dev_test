# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import argparse
from biocluster.config import Config
import subprocess
import shutil


parser = argparse.ArgumentParser(description="输入矩阵文件，生成树文件")
parser.add_argument('-l', "--linkage", help="聚类方式", required=True)
parser.add_argument("-i", "--matrix", help="输入的矩阵文件", required=True)
parser.add_argument("-o", "--output", help="输出文件的位置", required=True)
args = vars(parser.parse_args())

inFile = args["matrix"]
linkage = args["linkage"]
outFile = args["output"]
baseDir = os.path.dirname(outFile)
cmdPath = os.path.join(Config().SOFTWARE_DIR, "meta/scripts/beta_diversity/plot-hcluster_tree.pl")
Rpath = os.path.join(Config().SOFTWARE_DIR, "R-3.2.2/bin/R")

if not os.path.isfile(inFile):
    raise Exception("矩阵文件{}不存在".format(inFile))

try:
    with open(outFile, 'wb') as w:
        pass
except IOError:
    raise IOError('无法生成输出文件，请检查是否有输出路径的写入权限')

cmd = "{} -i {} -o {} -m {}".format(cmdPath, inFile, baseDir, linkage)
try:
    subprocess.check_call(cmd, shell=True)
except subprocess.CalledProcessError:
    raise Exception("运行plot-hcluster_tree.pl出错")

print "plot-hcluster_tree.pl完成"
Rcmd = "{} --restore --no-save < {}".format(Rpath, os.path.join(baseDir, "hc.cmd.r"))
try:
    print "开始运行生成的R脚本"
    subprocess.check_call(Rcmd, shell=True)
except subprocess.CalledProcessError:
    raise Exception("运行R脚本出错")

name = "hcluster_tree_{}_{}.tre".format(os.path.basename(inFile), linkage)
tmpTree = os.path.join(baseDir, name)
shutil.move(tmpTree, outFile)
