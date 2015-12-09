# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import os
import subprocess
import re
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.files.sequence.fastq_dir import FastqDirFile
from mbio.files.sequence.fasta_dir import FastaDirFile


class QcFormatAgent(Agent):
    """
    author: xuting
    last_modify: 2015.11.06
    接收fastq文件或者fastq文件夹，用于格式化用户的输入和产生一个fasta文件共下游OTU分析使用
    """
    def __init__(self, parent):
        super(QcFormatAgent, self).__init__(parent)
        options = [
            {'name': 'single_fastq', 'type': 'infile', 'format': 'sequence.fastq'},  # 输入的fastq文件
            {'name': 'multi_fastq', 'type': 'infile', 'format': 'sequence.fastq_dir'},  # 输入的fastq文件夹
            {'name': 'otu_fasta', 'type': 'outfile', 'format': 'sequence.fasta'},  # 输出的合并到一起的fasta，供后续的otu分析用
            {'name': 'renamed_fastq_dir', 'type': 'outfile', 'format': 'sequence.fastq_dir'},  # 按样本名进行重命名或者拆分的fastq文件夹
            {'name': 'fasta_dir', 'type': 'outfile', 'format': 'sequence.fasta_dir'}]  # 由fastq文件夹转化而来的fasta文件
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        """
        if self.option('single_fastq').is_set and self.option('multi_fastq').is_set:
            raise OptionError("请在参数single_fastq和multi_fastq之间选择一个进行输入！")
        if not (self.option('single_fastq').is_set or self.option('multi_fastq').is_set):
            raise OptionError("参数single_fastq和multi_fastq必须选择一个进行输入")
        if self.option('multi_fastq').is_set:
            if not self.option('multi_fastq').prop['has_list_file']:
                raise OptionError('multi_fastq参数中，fastq文件夹中必须含有一个名为list.txt的文件名--样本名的对应文件')
        if self.option('single_fastq').is_set:
            self.option('single_fastq').get_info()
            if not self.option('single_fastq').prop['has_sample_info']:
                raise OptionError("single_fastq参数中的fastq文件中必须在序列名中带有样本名称(以下划线分隔)")

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class QcFormatTool(Tool):
    def __init__(self, config):
        super(QcFormatTool, self).__init__(config)
        self.fastq_dir = os.path.join(self.work_dir, "output", "fastq_dir")
        self.fasta_dir = ''
        self.fasta = ''
        self.seq_sample = dict()

    def rename_fastq(self):
        """
        输入是文件夹的时候，根据文件名，样本名的对应文件，重命名文件
        """
        filename_sample = os.path.join(self.option('multi_fastq').prop['path'], "list.txt")
        with open(filename_sample, 'r') as r:
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                if re.search(r'\.(fastq|fq)\.gz', line[0]):
                    gz_file = os.path.join(self.option('multi_fastq').prop['path'], line[0])
                    target_file = os.path.join(self.fastq_dir, line[1] + ".fastq")
                    try:
                        subprocess.check_call('gunzip -c ' + gz_file + " >> " + target_file, shell=True)
                    except subprocess.CalledProcessError:
                        self.set_error("解压缩文件失败!检查输入是否是正确的gz文件")
                else:
                    file_ = os.path.join(self.option('multi_fastq').prop['path'], line[0])
                    target_file = os.path.join(self.fastq_dir, line[1] + ".fastq")
                    with open(file_, "r") as f:
                        txt = f.read()
                    with open(target_file, "a") as a:
                        a.write(txt)

    def seprate_fastq(self):
        """
        无对应文件，拆分fastq
        """
        warninglog = False
        count = 0
        with open(self.option('single_fastq').prop['path'], 'r') as f:
            for line in f:
                count += 1
                line = line.rstrip('\n')
                name = re.split('\s+', line)[0]
                name = re.sub(r'@', '', name)
                line = re.split(r'_', name)
                if len(line) > 2:
                    warninglog = True
                head = line[-1]
                line.pop(-1)
                filename = "_".join(line)
                filename = os.path.join(self.fastq_dir, filename + ".fastq")
                with open(filename, 'a') as a:
                    a.write("@" + head + "\n")
                    for i in range(1, 4):
                        line = f.next()
                        a.write(line)
                if count % 10000 == 0:
                    self.logger.info("正在输出第" + str(count) + "条序列")
        if warninglog:
            self.logger.warning("fastq文件里包含有两个以上的下划线，程序将取最后一个下划线之前的所有内容作为样本名！")
        self.logger.info("fastq 文件拆分完毕 ")

    def get_fastq_dir(self):
        """
        生成fastq文件夹
        """
        if not os.path.exists(self.fastq_dir):
            os.mkdir(self.fastq_dir)
        if self.option('multi_fastq').is_set:
            self.rename_fastq()
        if self.option('single_fastq').is_set:
            self.seprate_fastq()

    def get_fasta_dir(self):
        """
        生成fasta文件夹
        """
        fq_dir = FastqDirFile()
        fq_dir.set_path(self.fastq_dir)
        fq_dir.get_full_info(os.path.join(self.work_dir, "output"))
        try:
            self.fasta_dir = fq_dir.covert_to_fasta()
            self.logger.info("fasta 文件夹生成完毕")
        except Exception:
            self.set_error("fastq转化fasta失败！")

    def get_fasta(self):
        """
        生成fasta供otu分析
        """
        fa = FastaDirFile()
        fa.set_path(self.fasta_dir)
        fa.get_full_info(os.path.join(self.work_dir, "output"))
        try:
            self.fasta = fa.cat_fastas_for_meta()
            self.logger.info("后续OTU分析的fasta文件生成完毕")
        except Exception:
            self.set_error("生成fasta失败！")

    def set_output(self):
        self.logger.info("set output begin")
        self.option('otu_fasta').set_path(self.fasta)
        self.option('renamed_fastq_dir').set_path(self.fastq_dir)
        self.option('fasta_dir').set_path(self.fasta_dir)

    def run(self):
        """
        运行
        """
        super(QcFormatTool, self).run()
        self.get_fastq_dir()
        self.get_fasta_dir()
        self.get_fasta()
        self.set_output()
        self.logger.info("程序完成，即将退出")
        self.end()
