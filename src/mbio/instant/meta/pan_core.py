# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import subprocess
import shutil
from mbio.packages.meta.otu.pan_core_otu import pan_core
from biocluster.config import Config


class PanCoreInstant(object):
    def __init__(self, bindObject):
        self.logger = bindObject.logger
        self.option = bindObject.option
        self.work_dir = bindObject.work_dir
        self.R_path = os.path.join(Config().SOFTWARE_DIR, "R-3.2.2/bin/R")
        self._version = 1.0

    def run(self):
        self.create_pan_core()

    def create_pan_core(self):
        """
        用脚本pan_core_otu.py,生成pan_otu表格
        """
        self.logger.info("开始生成R脚本")
        if self.option["groupPath"] == "":
            panOtu = pan_core(self.option["otuPath"], "pan")
            coreOtu = pan_core(self.option["otuPath"], "core")
        else:
            panOtu = pan_core(self.option["otuPath"], "pan", self.option['groupPath'])
            coreOtu = pan_core(self.option["otuPath"], "core", self.option['groupPath'])
        print panOtu
        self.logger.info("R脚本生成完毕")
        self.logger.info("开始运行R,生成表格文件")
        try:
            panCmd = self.R_path + " --restore --no-save < " + panOtu
            coreCmd = self.R_path + " --restore --no-save < " + coreOtu
            subprocess.check_call(panCmd, shell=True)
            subprocess.check_call(coreCmd, shell=True)
            self.logger.info("表格生成完毕")
        except subprocess.CalledProcessError:
            self.logger.info("表格生成失败")
            raise Exception("运行R出错")
        tmpPan = os.path.join(self.work_dir, "pan.richness.xls")
        tmpCore = os.path.join(self.work_dir, "core.richness.xls")
        panDir = os.path.join(self.work_dir, "output", "pan.richness.xls")
        coreDir = os.path.join(self.work_dir, "output", "core.richness.xls")
        if os.path.exists(panDir):
            os.remove(panDir)
        if os.path.exists(coreDir):
            os.remove(coreDir)
        shutil.copy2(tmpPan, panDir)
        shutil.copy2(tmpCore, coreDir)
