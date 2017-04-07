# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from __future__ import division
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import re

class CogClassAgent(Agent):
    """
    cog功能分类
    """
    def __init__(self, parent):
        super(CogClassAgent, self).__init__(parent)
        options = [
           {"name": "diff_list", "type": "infile", "format": "denovo_rna.express.gene_list"},
            {"name": "cog_summary", "type": "infile", "format": "annotation.cog.cog_summary"}
        ]
        self.add_option(options)
        self.step.add_steps("cog_class")
        self.on('start', self.start_cog_class)
        self.on("end", self.end_cog_class)

    def start_cog_class(self):
        self.step.cog_class.start()
        self.step.update()

    def end_cog_class(self):
        self.step.cog_class.finish()
        self.step.update()

    def check_options(self):
        if not self.option("diff_list").is_set:
            raise OptionError("参数diff_list不能为空")

    def set_resource(self):
        self._cpu = 4
        self._memory = "4G"
        

class CogClassTool(Tool):
    def __init__(self, config):
        super(CogClassTool, self).__init__(config)
        self.gene_list = self.option("diff_list").prop["gene_list"]

    def cmd1(self):
        with open(self.option("cog_table").prop["path"], "r") as file:
            for line in file:
                line = line.strip()
                if line.startswith("#"):
                    continue
                tmp = line.split("\t")
                query = tmp[0]
                tmp[10]


    def run(self):
        super(CogClassTool, self).run()
        self.cmd1()