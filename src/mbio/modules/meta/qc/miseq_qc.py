# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import shutil
import os
from biocluster.module import Module
from biocluster.core.exceptions import OptionError


class MiseqQcModule(Module):
    def __init__(self, work_id):
        super(MiseqQcModule, self).__init__(work_id)
        options = [
            {'name': 'fastq_dir', 'type': 'infile', 'format': 'sequence.fastq_dir'},  # 输入的fastq文件夹
            {'name': 'filename_sample', 'type': 'infile', 'format': 'sequence.file_sample'},  # 文件名样品对应表
            {'name': 'fastq', 'type': 'infile', 'format': 'sequence.fastq'},  # 输入的fastq文件
            {'name': 'seqname_sample', 'type': 'infile', 'format': 'sequence.seq_sample'},  # 序列名样品对应表
            {'name': 'otu_fasta', 'type': 'outfile', 'format': 'sequence.fasta'}]  # 输出的合并到一起的fasta，供后续的otu分析用
        self.add_option(options)
        self.qc_format = self.add_tool('meta.qc.qc_format')
        self.base_info = self.add_tool('meta.qc.base_info')
        self.samples_info = self.add_tool('meta.qc.samples_info')
        self.reads_len_info = self.add_tool('meta.qc.reads_len_info')

    def check_options(self):
        """
        检查参数设置
        """
        if self.option('fastq_dir').is_set and self.option('fastq').is_set:
            raise OptionError("请在参数fastq_dir和fastq之间选择一个进行输入！")
        if not (self.option('fastq_dir').is_set or self.option('fastq').is_set):
            raise OptionError("参数fastq_dir和参数fastq必须选择一个进行输入")
        if self.option('fastq_dir').is_set:
            if not self.option('filename_sample').is_set:
                raise OptionError("输入fastq_dir参数后，必须输入filename_sample参数")
        self.logger.info("参数检测完毕")

    def qc_format_run(self):
        """
        运行qc_format，对输入的fastq文件夹或者fastq文件进行格式化
        生成fastq_dir,
        """
        if self.option('fastq_dir').is_set:
            myopt = {
                'fastq_dir': self.option('fastq_dir').prop['path'],
                'filename_sample': self.option('filename_sample').prop['path']
            }
        if self.option('fastq').is_set:
            if self.option('seqname_sample').is_set:
                myopt = {
                    'fastq': self.option('fastq').prop['path'],
                    'seqname_sample': self.option('seqname_sample').prop['path']
                }
            else:
                myopt = {
                    'fastq': self.option('fastq').prop['path']
                }
        self.qc_format.set_options(myopt)
        self.on_rely(self.qc_format, self.base_info_run)
        self.on_rely(self.qc_format, self.samples_info_run)
        self.on_rely(self.qc_format, self.reads_len_info_run)
        self.logger.info("开始对输入的fastq序列按样本进行格式化")
        self.qc_format.on('end', self.set_fasta)
        self.qc_format.run()

    def base_info_run(self):
        """
        运行碱基质量统计，对fastq文件夹里的fastq文件做碱基质量统计
        """
        self.logger.info("开始运行碱基质量统计")
        fastq_path = os.path.join(self.qc_format.work_dir, "output/fastq_dir")
        myopt = {
            'fastq_path': fastq_path
        }
        self.base_info.set_options(myopt)
        self.base_info.on('end', self.mv_base_info)
        self.base_info.run()

    def samples_info_run(self):
        """
        运行样品信息统计，对fasta文件夹进行样本信息统计
        """
        self.logger.info("开始进行样品信息统计")
        fasta_path = os.path.join(self.qc_format.work_dir, "output/converted_fastas")
        myopt = {
            'fasta_path': fasta_path
        }
        self.samples_info.set_options(myopt)
        self.samples_info.on('end', self.mv_samples_info)
        self.samples_info.run()

    def reads_len_info_run(self):
        """
        统计各个样本的序列长度分布
        """
        self.logger.info("开始进行序列长度分布统计")
        fasta_path = os.path.join(self.qc_format.work_dir, "output/converted_fastas")
        myopt = {
            'fasta_path': fasta_path
        }
        self.reads_len_info.set_options(myopt)
        self.reads_len_info.on('end', self.mv_reads_len)
        self.reads_len_info.run()

    def set_fasta(self):
        """
        设置合并到一起的fasta的文件路径
        """
        output = os.path.join(self.work_dir, "output")
        self.logger.info("设置fasta的文件路径")
        cat_fasta = os.path.join(self.qc_format.work_dir, "output/cat_meta.fasta")
        new_fasta = os.path.join(output, "cat_meta.fasta")
        if os.path.exists(new_fasta):
            os.remove(new_fasta)
        os.link(cat_fasta, new_fasta)
        self.option('otu_fasta').set_path(new_fasta)

    def mv_base_info(self):
        """
        移动base_info文件夹到output
        """
        output = os.path.join(self.work_dir, "output/base_info")
        if os.path.exists(output):
            os.rmdir(output)
        self.logger.info("开始移动baseinfo文件夹")
        base_info_dir = os.path.join(self.base_info.work_dir, "output/base_info")
        shutil.copytree(base_info_dir, output)

    def mv_reads_len(self):
        """
        移动长度统计文件到output
        """
        output = os.path.join(self.work_dir, "output/reads_len_info")
        if os.path.exists(output):
            os.rmdir(output)
        self.logger.info("开始移动长度分布统计文件夹")
        reads_len_info = os.path.join(self.reads_len_info.work_dir, "output/reads_len_info")
        shutil.copytree(reads_len_info, output)

    def mv_samples_info(self):
        """
        移动样品统计文件到output
        """
        output = os.path.join(self.work_dir, "output/samples_info")
        if os.path.exists(output):
            os.rmdir(output)
        self.logger.info("开始移动样品统计文件")
        samples_info_dir = os.path.join(self.samples_info.work_dir, "output/samples_info")
        shutil.copytree(samples_info_dir, output)

    def run(self):
        super(MiseqQcModule, self).run()
        self.qc_format_run()
        self.on_rely([self.base_info, self.samples_info, self.reads_len_info], self.end)
