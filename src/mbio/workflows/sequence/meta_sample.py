# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""
将fq文件进行拆分，并移入本地参考序列文件夹
"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import json
import shutil


class MetaSampleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaSampleWorkflow, self).__init__(wsheet_object)
        options = [
            {'name': 'in_fastq', 'type': 'infile',
             'format': 'sequence.fastq,sequence.fastq_dir'},  # 输入的fastq文件或fastq文件夹
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.fastq_extract = self.add_module("sample_base.fastq_extract")


    def check_options(self):
        """
        检查参数设置
        """
        return True

    def run(self):
        self.run_fastq_extract()
        self.end()
        super(MetaSampleWorkflow, self).run()

    def run_fastq_extract(self):
        opts = {
            "in_fastq" : self.option("in_fastq")
        }
        self.fastq_extract.set_options(opts)
        self.fastq_extract.run()

    def end(self):
        super(MetaSampleWorkflow, self).end()