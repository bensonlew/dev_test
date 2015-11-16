# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
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
    需要venn_table.py的package包
    需要R软件包
    """
    def __init_(self, parent):
        super(PanCoreOtuAgent, self).__init__(parent)
        options = [
            {"name": "otu_table", "type": "infile", "format": "otu_table"},  # 输入的OTU表格
            {"name": "group_table", "type": "infile", "format": "group_table"},  # 输入的group表格
            {"name": "pan_otu_table", "type": "outfile", "format": "pan_core_table"},  # 输出的pan_otu表格
            {"name": "core_otu_table", "type": "outfile", "format": "pan_core_table"}]  # 输出的core_otu表格
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("otu_table").is_set:
            raise OptionError("参数otu_table不能为空")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class PanCoreOtuTool(Tool):
    def __init__(self, config):
        super(PanCoreOtuTool, self).__init__(config)
        self.R_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/R")
        self._version = 1.0

    def _create_pan_core(self):
        """
        用脚本pan_core_otu.py,输出pan_otu表格
        """
        if self.option("group_table").is_set:
            group_table = self.option("group_table").prop['path']
            pan_otu = pan_core(self.option("otu_table"), "pan", group_table)
            core_otu = pan_core(self.option("otu_table"), "core", group_table)
        else:
            pan_otu = pan_core(self.option("otu_table"), "pan")
            core_otu = pan_core(self.option("otu_table"), "core")
        self.option("pan_otu_table").set_path(pan_otu)
        self.option("core_otu_table").set_path(core_otu)

    def run(self):
        """
        运行
        """
        super(PanCoreOtuTool, self).run()
        self._create_pan_core()
