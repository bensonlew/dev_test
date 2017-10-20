# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""
将fq文件进行拆分，并移入本地参考序列文件夹
"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fastq_dir import FastqDirFile
import os
import re
from biocluster.config import Config
import json
import shutil


class MetaSampleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaSampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"}, # 输入的序列文件或文件夹，仅新建样本集时用
            {"name": "info_file", "type": "infile", "format": "nipt.xlsx"},  # 样本信息文件，记录样本的合同号，引物等信息，仅新建样本集时用
            {"name": "file_list", "type": "infile", "format": "nipt.xlsx"},  # 从数据库dump下来的样本信息，仅重组样本集时用
            {"name": "sanger_type", "type": "string"},  # 判断sanger or tsanger
            {"name": "update_info", "type": "string"},
            {"name": "file_path", "type": "string"},   # 输入文件的磁盘路径
            {"name": "table_id", "type": "string"}  # 主表id
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.fastq_extract = self.add_module("sample_base.fastq_extract")
        self.fastq_recombined = self.add_tool("sample_base.fastq_recombined")

    def check_options(self):
        """
        检查参数设置
        """
        if self.option("in_fastq"):
            if self.option("info_file").is_set:
                pass
            else:
                raise OptionError("新建样本集时必须上传样本信息表！")
        elif self.option("file_list").is_set:
            pass
        else:
            raise OptionError("新建时必须上传fastq文件或文件夹，重组时必须有需要重组的样本信息！")

    def end(self):
        self.import2mongo()
        self.copy_data()
        super(MetaSampleWorkflow, self).end()

    def run_fastq_extract(self):
        opts = {
            "in_fastq": self.option("in_fastq")
        }
        self.fastq_extract.set_options(opts)
        self.fastq_extract.run()

    def run_fastq_recombined(self):
        opts = {
            "file_list": self.option("file_list")
        }
        self.fastq_recombined.set_options(opts)
        self.fastq_recombined.run()

    def import2mongo(self):
        self.logger.info("开始导入数据库")
        api_sample = self.api.sample_base
        sanger_path = Config().get_netdata_config(self.option('sanger_type'))
        root_path = sanger_path[self.option('sanger_type') + "_path"] + "/rerewrweset/sample_base"
        dir_path = root_path + "/" + self.option("table_id")
        table_id = self.option("table_id")
        sample_list = self.get_sample()
        if self.option("in_fastq").is_set:
            # 获得输入文件的路径
            if self.option("in_fastq").format == 'sequence.fastq':
                file_path = self.option("file_path")
            else:
                file_path = self.option("file_path")
            for sample in sample_list:
                sample_id = api_sample.add_sg_test_specimen_meta(sample,
                                                                 self.fastq_extract.option("output_list").prop["path"],
                                                                 self.option("info_file").prop["path"], dir_path, file_path)
                api_sample.add_sg_test_batch_specimen(table_id, sample_id, sample)
            api_sample.update_sg_test_batch_meta(table_id, self.option("info_file").prop["path"])  # 更新主表中的一些附属信息
        else:
            for sample in sample_list:
                sample_id = api_sample.add_sg_test_specimen_meta(sample,
                                                                 self.fastq_recombined.option("output_list").prop[
                                                                     "path"],
                                                                 self.fastq_recombined.option("info_file").prop["path"],
                                                                 dir_path)  # 导入样本信息
                api_sample.add_sg_test_batch_specimen(table_id, sample_id, sample)  # 导入样本信息关联表
            api_sample.update_sg_test_batch_meta(table_id,
                                                 self.fastq_recombined.option("info_file").prop["path"])  # 更新主表中的一些附属信息
        self.logger.info("完成导表")

    def get_sample(self):
        sample_list = []
        if self.option("in_fastq").is_set:
            dir_path = self.fastq_extract.option("output_fq").prop["path"]
        else:
            dir_path = self.fastq_recombined.output_dir
        all_files = os.listdir(dir_path)
        for files in all_files:
            m = re.match(r'(\S*)\.fq', files)
            if m:
                sample_list.append(m.group(1))
        return sample_list

    def copy_data(self):
        """
        将结果数据备份到磁盘中去
        :return:
        """
        sanger_path = Config().get_netdata_config(self.option('sanger_type'))
        root_path = sanger_path[self.option('sanger_type') + "_path"] + "/rerewrweset/sample_base"
        if os.path.exists(root_path):
            pass
        else:
            os.mkdir(root_path)
        if os.path.exists(root_path + '/' + self.option("table_id")):
            raise Exception('该样本集磁盘中已经存在，请核实')
        else:
            os.mkdir(root_path + '/' + self.option("table_id"))
            all_files = os.listdir(self.fastq_extract.option("output_fq").prop["path"])
            for fq_file in all_files:
                os.link(self.fastq_extract.option("output_fq").prop["path"] + "/" + fq_file,
                        root_path + '/' + self.option("table_id") + "/" + fq_file)

    def run(self):
        self.logger.info("开始运行！")
        if self.option("in_fastq"):
            self.fastq_extract.on("end", self.end)
            self.run_fastq_extract()
        else:
            self.fastq_recombined.on("end", self.end)
            self.run_fastq_recombined()
        super(MetaSampleWorkflow, self).run()