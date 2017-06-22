# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""sj测试工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
import os
import json
import shutil
import re


class SjWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(SjWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "genome_structure_file", "type": "infile", "format": "gene_structure.gff3"},
            {"name": "ref_genome", "type": "string", "default": "customer_mode"},  # 参考基因组
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.gs = self.add_tool("gene_structure.genome_structure")

    def check_options(self):
        """
        检查选项
        """
        return True

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def run_gs(self):
        opts = {
            "in_fasta": self.option("ref_genome_custom"),
            "in_gff": self.option("genome_structure_file")
        }
        self.gs.set_options(opts)
        self.gs.run()

    def run(self):
        # self.gs.on("end", self.end)
        # self.run_gs()

        super(SjWorkflow, self).run()

    def end(self):
        super(SjWorkflow, self).end()