# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""从序列文件或者序列文件夹当中获取样本信息"""
import os
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError


class SampleExtractWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(SampleExtractWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_fasta", "type": "infile", "format": "sequence.fasta, sequence.fasta_dir"},
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq, sequence.fastq_dir"},
            {"name": "table_id", "type": "string"},
            {"name": "update_info", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.seq_extract = None
        self.setted_option = ""
        self.check_options()  # 通常不应该直接在调用， 但这里有些特殊，需要确定in_fasta和in_fastq哪个进行了设定
        if self.setted_option == "fasta":
            self.seq_extract = self.add_tool("sequence.fasta_sample_extract")
        elif self.setted_option == "fastq":
            self.seq_extract = self.add_module("meta.sample_extract.sample_extract")

    def check_options(self):
        # if self.option("in_fasta").is_set and self.option("in_fastq").is_set
        # 由于多文件format的问题， 当两个文件format检测都没有通过的时候，
        # 这个option的值是bool， is_set属性不能使用
        # 所以在这里，当option的值是bool的时候， 就说明这个变量没有设置
        if not isinstance(self.option("in_fastq"), bool) and not isinstance(self.option("in_fasta"), bool):
            raise OptionError("不能同时设置参数in_fasta和in_fastq")
        if isinstance(self.option("in_fastq"), bool):
            try:
                self.option("in_fasta").is_set
                self.setted_option = "fasta"
            except Exception as e:
                raise OptionError("参数in_fasta错误:{}".format(e))
        if isinstance(self.option("in_fasta"), bool):
            try:
                self.option("in_fastq").is_set
                self.setted_option = "fastq"
            except Exception as e:
                raise OptionError("参数in_fastq错误:{}".format(e))

    def run_seq_extract(self):
        if self.setted_option == "fasta":
            opts = {
                "in_fasta": self.option("in_fasta")
            }
        elif self.setted_option == "fastq":
            opts = {
                "in_fastq": self.option("in_fastq"),
                "table_id": self.option("table_id")
            }
        self.seq_extract.set_options(opts)
        self.seq_extract.on("end", self.set_db)
        self.seq_extract.run()

    def set_db(self):
        api_sample = self.api.sample_extract
        api_sample.update_sg_seq_sample(self.seq_extract.option("file_sample_list").prop["path"], self.option("table_id"))
        self.end()

    def run(self):
        self.run_seq_extract()
        super(SampleExtractWorkflow, self).run()
