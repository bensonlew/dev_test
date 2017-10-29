# -*- coding: utf-8 -*-
# __author__ = 'shijin'
# __editor__ = 'wangzhaoyue' 20171025

"""
将fq文件进行拆分，并移入本地参考序列文件夹
"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
from biocluster.config import Config
import json


class SampleBaseWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(SampleBaseWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq,sequence.fastq_dir"},  # 输入的序列文件或文件夹，仅新建样本集时用
            {"name": "info_file", "type": "infile", "format": "sequence.sample_base_table"},  # 样本信息文件，记录样本的合同号，引物等信息，仅新建样本集时用
            {"name": "file_list", "type": "infile", "format": "nipt.xlsx"},  # 从数据库dump下来的样本信息，仅重组样本集时用
            {"name": "sanger_type", "type": "string"},  # 判断sanger or tsanger
            {"name": "update_info", "type": "string"},
            {"name": "file_path", "type": "string"},   # 输入文件的磁盘路径，磁盘路径也文件别名的list
            {"name": "table_id", "type": "string"},  # 主表id
            {"name": "pipeline_type", "type": "string"}  # 流程
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
            if self.option("info_file"):
                pass
            else:
                raise OptionError("新建样本集时必须上传样本信息表！")
        elif self.option("file_list"):
            pass
        else:
            raise OptionError("新建时必须上传fastq文件或文件夹，重组时必须有需要重组的样本信息！")

    def end(self):
        self.set_output()
        self.import2mongo()
        self.copy_data()
        super(SampleBaseWorkflow, self).end()

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
        if self.option("in_fastq"):
            file_path = json.loads(self.option("file_path"))
            for sample in sample_list:
                self.logger.info(self.option("info_file").prop["new_table"])
                sample_id, file_alias_list = api_sample.add_sg_test_specimen_meta(sample,
                                                                 self.fastq_extract.option("output_list").prop["path"],
                                                                 self.option("info_file").prop["new_table"], dir_path, file_path)
                api_sample.add_sg_test_batch_specimen(table_id, sample_id, sample, file_alias_list)
            api_sample.update_sg_test_batch_meta(table_id, self.option("info_file").prop["new_table"])  # 更新主表中的一些附属信息
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
        dir_path = self.output_dir + '/fastq/'
        all_files = os.listdir(dir_path)
        for files in all_files:
            m = re.match(r'(\S*)\.fq', files)
            if m:
                sample_list.append(m.group(1))
        return sample_list

    def set_output(self):
        """
        将结果数据整理到workflow的结果文件夹中
        :return:
        """

        new_fastq_path = self.output_dir + '/fastq/'
        if os.path.exists(new_fastq_path):
            os.remove(new_fastq_path)
        os.mkdir(new_fastq_path)
        if self.option("in_fastq"):
            fastq_path = self.fastq_extract.output_dir + '/fastq/'
            allfiles = os.listdir(fastq_path)
            oldfiles = [os.path.join(fastq_path, i) for i in allfiles]
            newfiles = [os.path.join(new_fastq_path, i) for i in allfiles]
            for newfile in newfiles:
                if os.path.exists(newfile):
                    if os.path.isfile(newfile):
                        os.remove(newfile)
                    else:
                        os.system('rm -r %s' % newfile)
            for i in range(len(allfiles)):
                if os.path.isfile(oldfiles[i]):
                    os.link(oldfiles[i], newfiles[i])
                elif os.path.isdir(oldfiles[i]):
                    os.link(oldfiles[i], self.output_dir)
            os.link(self.fastq_extract.output_dir + '/info.txt', self.output_dir + '/info.txt')
        else:
            dir_path = self.fastq_recombined.output_dir
            allfiles = os.listdir(dir_path)
            for files in allfiles:
                old_file = os.path.join(dir_path, files)
                if files.endswith(".fq"):
                    new_file = os.path.join(new_fastq_path, files)
                else:
                    new_file = os.path.join(self.output_dir, files)
                os.link(old_file, new_file)

    def copy_data(self):
        """
        将结果序列文件备份到磁盘中去
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
            all_files = os.listdir(self.output_dir + '/fastq/')
            for fq_file in all_files:
                os.link(self.output_dir + '/fastq/' + fq_file, root_path + '/' + self.option("table_id") + "/" + fq_file)

    def run(self):
        self.logger.info("开始运行！")
        if self.option("pipeline_type") == 'meta':
            if self.option("in_fastq"):
                self.fastq_extract.on("end", self.end)
                self.run_fastq_extract()
            else:
                self.fastq_recombined.on("end", self.end)
                self.run_fastq_recombined()
        super(SampleBaseWorkflow, self).run()