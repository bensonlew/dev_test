# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import subprocess
import shutil
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from mbio.packages.meta.otu.pan_core_otu import pan_core


class PanCoreOtuAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.12
    需要pan_core_otu.py的package包
    需要R软件包
    """
    def __init__(self, parent):
        super(PanCoreOtuAgent, self).__init__(parent)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},  # 输入的OTU文件
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},  # 输入的group表格
            {"name": "level", "type": "string", "default": "otu"},  # 物种水平
            {"name": "pan_otu_table", "type": "outfile", "format": "meta.otu.pan_core_table"},  # 输出的pan_otu表格
            {"name": "core_otu_table", "type": "outfile", "format": "meta.otu.pan_core_table"}]  # 输出的core_otu表格
        self.add_option(options)
        self.step.add_steps("create_pan_core")
        self.on('start', self.start_pan_core)
        self.on('end', self.end_pan_core)

    def start_pan_core(self):
        self.step.create_pan_core.start()
        self.step.update()

    def end_pan_core(self):
        self.step.create_pan_core.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("in_otu_table").is_set:
            raise OptionError("参数otu_table不能为空")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class',
                                        'order', 'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        return True

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            [r"pan.richness.xls", "xls", "pan表格"],
            [r"core.richness.xls", "xls", "core表格"]
        ])

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class PanCoreOtuTool(Tool):
    def __init__(self, config):
        super(PanCoreOtuTool, self).__init__(config)
        self.R_path = os.path.join(Config().SOFTWARE_DIR, "R-3.2.2/bin/R")
        self._version = 1.0

    def _create_pan_core(self):
        """
        用脚本pan_core_otu.py,输出pan_otu表格
        """
        self.logger.info("开始生成R脚本")
        otu_path = ""
        if self.option("in_otu_table").format == "meta.otu.tax_summary_dir":
            otu_path = self.option("in_otu_table").get_table(self.option("level"))
        else:
            otu_path = self.option("in_otu_table").prop['path']

        # 检测otu表行数
        num_lines = sum(1 for line in open(otu_path))
        if num_lines < 11:
            self.set_error("Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！")
            raise Exception("Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！")

        if self.option("group_table").is_set:
            group_table = self.option("group_table").prop['path']
            pan_otu = pan_core(otu_path, "pan", group_table)
            core_otu = pan_core(otu_path, "core", group_table)
        else:
            pan_otu = pan_core(otu_path, "pan")
            core_otu = pan_core(otu_path, "core")
        self.logger.info("R脚本生成完毕")
        self.logger.info("运行R,生成表格文件")
        try:
            pan_cmd = self.R_path + " --restore --no-save < " + pan_otu
            core_cmd = self.R_path + " --restore --no-save < " + core_otu
            subprocess.check_call(pan_cmd, shell=True)
            subprocess.check_call(core_cmd, shell=True)
            self.logger.info("表格生成完毕")
        except subprocess.CalledProcessError:
            self.logger.info("表格生成失败")
            raise Exception("运行R出错")
        tmp_pan = os.path.join(self.work_dir, "pan.richness.xls")
        tmp_core = os.path.join(self.work_dir, "core.richness.xls")
        pan_dir = os.path.join(self.work_dir, "output", "pan.richness.xls")
        core_dir = os.path.join(self.work_dir, "output", "core.richness.xls")
        if os.path.exists(pan_dir):
            os.remove(pan_dir)
        if os.path.exists(core_dir):
            os.remove(core_dir)
        shutil.copy2(tmp_pan, pan_dir)
        shutil.copy2(tmp_core, core_dir)
        self.option("pan_otu_table").set_path(pan_dir)
        self.option("core_otu_table").set_path(core_dir)

    def run(self):
        """
        运行
        """
        super(PanCoreOtuTool, self).run()
        self._create_pan_core()
        self.logger.info("程序完成，即将退出")
        self.end()
