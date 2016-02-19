# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class FileCheckAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2016.02.17
    用于在workflow开始之前对所输入的文件进行详细的内容检测
    """
    def __init__(self, parent):
        super(FileCheckAgent, self).__init__(parent)
        options = [
            {"name": "in_fastq", "type": "infile", 'format': "sequence.fastq, sequence.fastq_dir"},
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "env_table", "type": "infile", "format": "meta.otu.group_table"},
            {'name': 'ref_fasta', 'type': 'infile', 'format': 'sequence.fasta'},  # 参考fasta序列
            {'name': 'ref_taxon', 'type': 'infile', 'format': 'taxon.seq_taxon'}  # 参考taxon文件
        ]
        self.add_option(options)
        self.step.add_steps("file_check")
        self.on('start', self.start_file_check)
        self.on('end', self.end_file_check)

    def start_file_check(self):
        self.step.file_check.start()
        self.step.update()

    def end_file_check(self):
        self.step.file_check.finish()
        self.step.update()

    def check_option(self):
        if not self.option('in_fastq').is_set:
            raise OptionError("必须输入in_fastq参数")
        self.option('in_fastq').get_info()
        if self.get_option_object("in_fastq").format == 'sequence.fastq_dir':
            if not self.option('in_fastq').prop['has_list_file']:
                raise OptionError('fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')
        if self.get_option_object('in_fastq').format == 'sequence.fastq':
            if not self.option('in_fastq').prop['has_sample_info']:
                raise OptionError("fastq文件中必须在序列名中带有样本名称(以下划线分隔)")
        if self.option("ref_fasta").is_set:
            if not self.option("ref_taxon").is_set:
                raise OptionError("检测到ref_fasta， 但未检测到ref_taxon")
        if self.option("ref_taxon").is_set:
            if not self.option("ref_fasta").is_set:
                raise OptionError("检测到ref_taxon， 但未检测到ref_fasta")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class FileCheckTool(Tool):
    def __init__(self, config):
        super(FileCheckTool, self).__init__(config)
        self.samples = list()

    def check_fastq(self):
        self.logger.info("正在检测fastq文件")
        self.option('in_fastq').get_info()
        if self.get_option_object("in_fastq").format == 'sequence.fastq_dir':
            self.samples = self.option('in_fastq').prop["samples"]
        if self.get_option_object('in_fastq').format == 'sequence.fastq':
            self.option("in_fastq").check_content()
            self.samples = self.option('in_fastq').prop["samples"]

    def check_group(self):
        if self.option("group_table"):
            self.logger.info("正在检测group文件")
            self.option("group_table").get_info()
            gp_sample = self.option("group_table").prop["sample"]
            for gp in gp_sample:
                if gp not in self.samples:
                    raise Exception("group表出错, 样本{}在fastq文件中未出现".format(gp))
        else:
            self.logger.info("未检测到group文件， 跳过...")

    def check_ref(self):
        if self.option("ref_fasta").is_set:
            self.logger.info("开始校验ref_fasta和ref_taxon文件")
            fasta_name = self.option("ref_fasta").get_all_seq_name()
            taxon_name = self.option("ref_taxon").get_all_name()
            for f_name in fasta_name:
                if f_name not in taxon_name:
                    raise Exception("序列名{}在taxon文件里未出现")
            if len(fasta_name) != len(taxon_name):
                raise Exception("ref_taxon文件里的某些序列名在ref_fatsa里未找到")
        else:
            self.logger.info("未检测到ref_fasta和ref_taxon文件， 跳过...")

    def check_env(self):
        if self.option("env_table").is_set:
            self.logger.info("开始校验env文件")
            self.option("env_table").check()
        else:
            self.logger.info("未检测到env文件， 跳过...")

    def run(self):
        super(FileCheckTool, self).run()
        self.check_fastq()
        self.check_group()
        self.check_ref()
        self.check_env()
        self.end()
