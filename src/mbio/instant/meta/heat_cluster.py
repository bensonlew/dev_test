# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.config import Config
import subprocess


class HeatClusterInstant(object):
    def __init__(self, bindObject):
        self.logger = bindObject.logger
        self.option = bindObject.option
        self.work_dir = bindObject.work_dir
        self.matrixScriptPath = os.path.join(Config().SOFTWARE_DIR, "bioinfo/statistical/script/distance_calc.py")
        self.hclusterScriptPath = os.path.join(Config().SOFTWARE_DIR, "bioinfo/statistical/script/hcluster.py")

    def run(self):
        self.createDistanceMatrix()
        self.createTree()

    def createDistanceMatrix(self):
        self.logger.info("开始生成距离矩阵文件")
        try:
            distanceMatrixPath = os.path.join(self.work_dir, "otu.matrix")
            matrixCmd = "python {} -m bray_curtis -i {} -o {}".format(self.matrixScriptPath, self.option["otuPath"], distanceMatrixPath)
            print matrixCmd
            subprocess.check_call(matrixCmd, shell=True)
            self.logger.info("距离矩阵文件otu.matrix生成成功")
        except subprocess.CalledProcessError:
            self.logger.info("距离矩阵生成失败")
            raise Exception("距离矩阵生成失败")

    def createTree(self):
        self.logger.info("开始生成树文件")
        try:
            treePath = os.path.join(self.work_dir, "tree.txt")
            treeeCmd = "python {} -l {} -i {} -o {}".format(self.hclusterScriptPath, self.option["linkage"], os.path.join(self.work_dir, "otu.matrix"), treePath)
            print treeeCmd
            subprocess.check_call(treeeCmd, shell=True)
            self.logger.info("树文件tree.txt生成成功")
        except subprocess.CalledProcessError:
            self.logger.info("树文件生成失败")
            raise Exception("树文件生成失败")


