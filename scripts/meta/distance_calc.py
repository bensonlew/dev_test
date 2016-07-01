# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import argparse
from biocluster.config import Config
from mbio.files.meta.otu.otu_table import OtuTableFile
import subprocess
import shutil

parser = argparse.ArgumentParser(description='输入计算方法(必须)和树文件(非必须), 计算距离矩阵')
parser.add_argument('-m', '--method', help="距离矩阵的计算方法", required=True)
parser.add_argument('-t', "--TreeFile", help="树文件的位置")
parser.add_argument('-i', "--otuFile", help="输入的otu文件的位置", required=True)
parser.add_argument('-o', "--output", help="输出的矩阵文件位置", required=True)
args = vars(parser.parse_args())


inFile = args["outFile"]
method = args["method"]
outFile = args["output"]
baseDir = os.path.dirname(outFile)
biomPath = os.path.join(baseDir, "otuTmp.Biom")
cmdPath = os.path.join(Config().SOFTWARE_DIR, "Python/bin/beta_diversity.py")

if not os.path.isfile(inFile):
    raise Exception("otu文件{}不存在".format(inFile))

try:
    with open(outFile, 'wb') as w:
        pass
except IOError:
    raise IOError('无法生成输出文件，请检查是否有输出路径的写入权限')

OtuFile = OtuTableFile()
OtuFile.set_path(inFile)
OtuFile.check()
OtuFile.convert_to_biom(biomPath)
cmd = "{} -m {} -i {} -o {}".format(cmdPath, method, biomPath, baseDir)

try:
    subprocess.check_call(cmd)
except subprocess.CalledProcessError:
    raise Exception("运行beta_diversity.py出错")

matrixName = os.path.join(baseDir, method + "_otuTmp.txt")
shutil.move(matrixName, outFile)
