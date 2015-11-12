# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from venn_table import venn_table


class VennTableAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.11
    需要R软件
    需要venn_table.py的package包
    """
    def __init__(self, parent):
        super(VennTableAgent, self).__init__(parent)
        options = [
            {"name": "otu_table", "type": "infile", "format": "otu_table"},  # 输入的OTU表格
            {"name": "group_table", "type": "infile", "format": "group_table"},  # 输入的group表格
            {"name": "venn_table", "type": "outfile", "format": "venn_table"}]  # 输入的Venn表格
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("otu_table").is_set:
            raise OptionError(u"参数otu_table不能为空")
        if not self.option("group_table").is_set:
            raise OptionError(u"参数group_table不能为空")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class VennTableTool(Tool):
    def __init__(self, config):
        super(VennTableTool, self).__init__(config)
        self.R_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/R")
        self._version = 1.0

    def _create_venn_table(self):
        """
        调用脚本venn_table.py,输出venn表格
        """
        venn_path = venn_table(self.option("otu_table"), self.option("group_table"), self.R_path)
        self.option("venn_table").set_path(venn_path)

    def run(self):
        """
        运行
        """
        super(VennTableTool, self).run()
        self._create_venn_table()
