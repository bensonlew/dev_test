# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import copy
from biocluster.agent import Agent
from biocluster.tool import Tool
from mbio.files.sequence.fasta import FastaFile
from biocluster.core.exceptions import OptionError


class SamplesInfoAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.06
    """
    def __init__(self, parent):
        super(SamplesInfoAgent, self).__init__(parent)
        options = [
            {"name": "fasta_path", "type": "infile", "format": "sequence.fasta_dir"},  # 输入文件，与fastq_path二选一
            {"name": "fastq_path", "type": "infile", "format": "sequence.fastq_dir"},  # 输入文件，与fasta_path二选一
            {"name": "sample_number", "type": "int", "default": 0},  # 项目中包含的样本的数目，应当和输入文件夹中的fsta或者fastq文件的数目一致，用于检查是否有样本遗漏
            {"name": "out_fasta_path", "type": "outfile", "format": "sequence.fasta_dir"},  # 输出的fasta_dir文件夹,供后续分析,
                                                                                            # 当输入文件是"fastq_path",输出转化的fasta_dir;
                                                                                            # 当输入文件为fasta_path时，直接输出
            {"name": "samples_info", "type": "outfile", "format": "meta.qc.samples_info"}]  # 输出的samples_info文件
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not (self.option("fasta_path").is_set or self.option("fastq_path").is_set):
            raise OptionError("参数fasta_path和参数fastq_path不能都为空")
        # 设置文件夹的文件数目，并检测与实际的数目是否一致
        if self.option("fastq_path").is_set:
            self.option("fastq_path").set_file_number(self.option("sample_number"))
            self.option("fastq_path").check()
        if self.option("fasta_path").is_set:
            self.option("fasta_path").set_file_number(self.option("sample_number"))
            self.option("fasta_path").check()
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class SamplesInfoTool(Tool):
    def __init__(self, config):
        super(SamplesInfoTool, self).__init__(config)
        self._version = 1.0
        self.logger.warning('running')
        if self.option("fastq_path").is_set:
            self.option("fastq_path").get_full_info(os.path.join(self.work_dir, 'fastq_work_dir'))

    def create_table(self):
        """
        生成samples_info表
        """
        self.logger.info('生成fasta文件夹')
        self._get_fasta_dir()
        self.logger.info('成功生成fasta文件夹,开始统计样本信息')
        file_name = os.path.join(self.output_dir, self.id + ".samples_info")
        self.option("out_fasta_path").get_full_info(os.path.join(self.work_dir, 'fasta_work_dir'))
        with open(file_name, "w") as f:
            head = ["sample", "reads", "bases", "avg", "min", "max"]
            f.write("\t".join(head) + "\n")
            for fasta in self.option("out_fasta_path").prop["fasta_fullname"]:
                fastafile = FastaFile()
                fastafile.set_path(fasta)
                if fastafile.check():
                    info_ = list()
                    info_.append(fastafile.prop["sample_name"])
                    info_.append(fastafile.prop["seq_number"])
                    info_.append(fastafile.prop["bases"])
                    avg = int(fastafile.prop["bases"]) / int(fastafile.prop["seq_number"])
                    avg = str(avg)
                    info_.append(avg)
                    info_.append(fastafile.prop["shortest"])
                    info_.append(fastafile.prop["longest"])
                    f.write("\t".join(info_) + "\n")
        self.option("samples_info").set_path(file_name)
        self.logger.info('样本信息统计完毕！')

    def _get_fasta_dir(self):
        """
        获取输出的fasta文件夹
        将fastq文件夹转化成fasta文件夹
        """

        if self.option("fasta_path").is_set:
            self.option("out_fasta_path").value(copy.deepcopy(self.option("fasta_path")))
        else:
            new_fasta_path = self.option("fastq_path").covert_to_fasta()
            self.option("out_fasta_path").set_path(new_fasta_path)

    def run(self):
        """
        运行
        """
        super(SamplesInfoTool, self).run()
        self.create_table()
        self.logger.info("退出样本信息统计模块")
        self.end()
