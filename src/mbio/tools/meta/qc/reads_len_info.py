# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import os
from collections import defaultdict
from Bio import SeqIO
from biocluster.agent import Agent
from biocluster.tool import Tool
from mbio.files.fasta import FastaFile
from biocluster.core.exceptions import OptionError


class ReadsLenInfoAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.09
    """
    def __init__(self, parent):
        """
        :param longset:所有的fasta中最长的那条序列
        :param allow_step:允许的步长
        """
        super(ReadsLenInfoAgent, self).__init__(parent)
        options = [{"name": "fasta_path", "type": "infile", "format": "fasta_dir"},  # 输入的fasta文件夹
                   {"name": "sample_number", "type": "string"},  # 项目中包含的样本的数目，应当和输入文件夹中的fsta或者fastq文件的数目一致，用于检查是否有样本遗漏
                   {"name": "reads_len_info", "type": "outfile", "format": "reads_len_info_dir"}]  # 输出的reads_len_info文件夹
        self.add_option(options)
        self.longest = ""
        self.allow_step = [1, 20, 50, 100]

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("fasta_path").is_set:
            raise OptionError(u"参数fasta_path不能都为空")
        if not self.option("sample_number").is_set:
            raise OptionError(u"必须设置参数sample_number")
        # 设置文件夹的文件数目，并检测与实际的数目是否一致
        self.option("fasta_path").set_file_number(self.option("sample_number"))
        self.option("fasta_path").check()
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class ReadsLenInfoTool(Tool):
    """
    """
    def __init__(self, config):
        super(ReadsLenInfoTool, self).__init__(config)
        self._version = 1.0

    def _create_reads_len_info(self):
        """
        生成4个reads_len_info文件
        """
        tmp_dir = os.path.join(self.option("fasta_path").prop["path"], "reads_len_info")
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        self.option("reads_len_info").set_path(tmp_dir)
        # 寻找最长的序列
        max_list = list()
        for fasta in self.option("fasta_path").fasta_full:
            # 获取每一个fasta的全路径
            myfasta = FastaFile()
            myfasta.set_path(fasta)
            max_list.append(int(myfasta.prop["longest"]))
        self.longest = max(max_list)
        for i in self.allowed_step:
            self._write_head(i)
        for fasta in self.option("fasta_path").fasta_full:
            self._len_stat(fasta)

    def _write_head(self, step):
        """
        往reads_len_info文件里输出表头
        """
        file_name = os.path.join(self.option("reads_len_info").prop["path"],
                                 "step_" + step + ".reads_len_info")
        with open(file_name, "w") as f:
            f.write("sample" + "\t")
            col = self.longest // step
            head = list()
            for i in range(step, col, step):
                if step == 1:
                    str_ = str(i - step + 1)
                else:
                    str_ = str(i - step + 1) + "-" + str(i)
                head.append(str_)
            f.write("\t".join(head) + "\n")

    def _len_stat(self, fasta):
        """
        统计一个fasta文件的长度分布, 往不同的step文件里输出一行(一个样本的长度分布）
        :param step_1: 用于记录步长为1的reads的分布信息
        :param step_20: 用于记录步长为20的reads的分布信息 例如区间21-40 对应的应该是step_20[40]
        :param step_50: 用于记录步长为50的reads的分布信息
        :param step_100: 用于记录步长为100的reads的分布信息
        """
        self.step_1 = defaultdict()
        self.step_20 = defaultdict()
        self.step_50 = defaultdict()
        self.step_100 = defaultdict()
        for seq in SeqIO.parse(fasta, "fasta"):
            len_ = len(seq.seq)
            for i in self.allow_step:
                self._find_range(len_, i, eval("self.step_" + "i"))
        for mystep in self.allow_step:
            self._write_len_info(mystep, eval("self.step_" + "i"))

    def _write_len_info(self, step, dict_):
        """
        往step_1.reads_len_info;step_20.reads_len_info;step_50.reads_len_info;step_100.reads_len_info
        输出一行
        """
        i = step
        file_name = os.path.join(self.option("reads_len_info").prop["path"],
                                 "step_" + step + ".reads_len_info")
        with open(file_name, "w") as f:
            temp_list = list()
            for i in range(i, self.longest, step):
                temp_list.append(dict_[i])
            f.write("\t".join(temp_list) + "\n")

    @staticmethod
    def _find_range(len_, step, dict_):
        """
        计算某一个长度序列应该属于哪个区间，并将相应的dict 加1
        例如某条序列 长度len_为32，要计算步长20时，属于哪个区间，则传入参数应当是(32, 20, step_20)
        最后计算可知32 属于21-40的区间，字典step_20[40]应当加1
        """
        i = step
        while True:
            if i // len_ == 1:
                dict_[i] += 1
                break
            i += step

    def run(self):
        """
        运行
        """
        super(ReadsLenInfoTool).run()
        self._create_reads_len_info()
