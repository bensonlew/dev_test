# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""
将fq文件进行拆分，并移入本地参考序列文件夹
"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fastq_dir import FastqDirFile
import os
import json
import shutil


class MetaSampleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaSampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},
            {"name": "fq_type", "type": "string", "default": "PE"},
            {"name": "update_info", "type": "string"},
            {"name": "table_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.fastq_extract = self.add_module("sample_base.fastq_extract")
        # self.updata_status_api = self.api.meta_update_status

    def check_options(self):
        """
        检查参数设置
        """
        return True

    def end(self):
        self.import2mongo()
        super(MetaSampleWorkflow, self).end()

    def run(self):
        self.logger.info("开始运行！！！")
        self.fastq_extract.on("end", self.end)     # change by wzy 20170925, 加上依赖关系
        self.run_fastq_extract()
        super(MetaSampleWorkflow, self).run()
        # self.end()  # change by wzy 20170925, 注释掉

    def import2mongo(self):
        self.logger.info("开始导入数据库")
        api_sample = self.api.sample_base
        try:
            table_id = self.option("table_id")
        except:
            table_id = "test_01"
        sample_list = self.get_sample()
        for sample in sample_list:
            sample_id = api_sample.add_sg_test_specimen_meta(sample, self.fastq_extract.option("output_list").prop["path"],
                                                        self.file_sample)
            api_sample.add_sg_test_batch_specimen(table_id, sample_id, sample)
    #
    def get_sample(self):
        dir_path = self.fastq_extract.option("output_fq").prop["path"]
        dir = FastqDirFile()
        dir.set_path(dir_path)
        dir.check()
        sample_list = dir.prop["samples"]
        self.file_sample = dir.prop["file_sample"]
        self.logger.info(str(sample_list))
        return sample_list

    def run_fastq_extract(self):
        opts = {
            "in_fastq": self.option("in_fastq")
        }
        self.fastq_extract.set_options(opts)
        self.fastq_extract.run()