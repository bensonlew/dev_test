# -*- coding: utf-8 -*-
# __author__ = 'sj & hesheng'

from __future__ import division
import os
import re
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from collections import defaultdict
import resource


class FastqSampleExtractAgent(Agent):
    """
    从fastq或者fastq文件夹里提取样本的信息
    """
    def __init__(self, parent):
        super(FastqSampleExtractAgent, self).__init__(parent)
        options = [
            {"name": "in_fastq", "type": "infile", "format": "sequence.fastq"},
            {"name": "file_sample_list", "type": "outfile", "format": "sequence.info_txt"},
            {"name": "out_fa", "type": "outfile", "format": "sequence.fasta_dir"},
            {"name": "length_dir", "type": "outfile", "format": "sequence.length_dir"}
        ]
        self.add_option(options)
        self.step.add_steps("sample_extract")
        self.on('start', self.start_sample_extract)
        self.on("end", self.end_sample_extract)

    def start_sample_extract(self):
        self.step.sample_extract.start()
        self.step.update()

    def end_sample_extract(self):
        self.step.sample_extract.finish()
        self.step.update()

    def check_options(self):
        if not self.option("in_fastq").is_set:
            raise OptionError("参数in_fastq不能为空")

    def set_resource(self):
        self._cpu = 4
        self._memory = "4G"


class Sample(object):
    def __init__(self, name, fasta_dir='./mytest2/', length_dir='./mytest2/'):
        self.name = name
        self.total_base_num = 0
        self.seqs_len = defaultdict(int)
        self._new_fasta_file = open(fasta_dir + '/' + self.name + '.fasta', 'w')
        self.length_dir = length_dir

    def add_new_fasta(self, seq, seq_name):
        self._new_fasta_file.write('>{}\n{}'.format(seq_name, seq))
        length = len(seq) - 1  # 减去一个回车符
        self.seqs_len[length] += 1
        self.total_base_num += length

    def get_info(self):
        self.seqs_num = sum(self.seqs_len.values())
        self.seqs_mean_len = self.total_base_num / self.seqs_num
        self.seqs_min_len = min(self.seqs_len.keys())
        self.seqs_max_len = max(self.seqs_len.keys())

    def write_len(self):
        self._new_seq_len_file = open(self.length_dir + '/' + self.name + '.length_file', 'w')
        for length in sorted(self.seqs_len.keys()):
            self._new_seq_len_file.write('\n'.join([str(length)] * self.seqs_len[length]) + '\n')

    def close_all(self):
        self._new_fasta_file.close()
        self._new_seq_len_file.close()


class FastqSampleExtractTool(Tool):
    def __init__(self, config):
        super(FastqSampleExtractTool, self).__init__(config)
        self.samples = {}
        self.logger.debug(resource.getrlimit(resource.RLIMIT_NOFILE))

    def parse_fastq(self, f_path):
        with open(f_path) as fastq:
            for line in fastq:
                m = re.match("@(.+)_(\d+)", line)
                if not m:
                    self.set_error("fastq文件格式不符合要求，第一行形式应为应为@样本名_序列号")
                    raise Exception('fastq文件格式不符合要求，第一行形式应为应为@样本名_序列号')
                sample_name = m.group(1)
                # if sample_name.find(".") != -1:
                #     raise Exception("样本名称中含有.，请更改样本名称后再进行工作流分析")
                seq_name = m.group(2)
                sample = self.return_sample(sample_name)
                sample.add_new_fasta(next(fastq), seq_name)
                try:
                    next(fastq)
                    next(fastq)
                except:
                    self.set_error("fastq文件缺失，请检查后几行文件是否完整")
                    raise Exception("fastq文件缺失，请检查后几行文件是否完整")
        with open('info.txt', 'w') as info:
            info.write('#file\tsample\tworkdir\tseqs_num\tbase_num\tmean_length\tmin_length\tmax_length\n')
            for sample in self.samples.values():
                sample.get_info()
                sample.write_len()
                sample.close_all()
                info.write('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n'.format(self.option("in_fastq").prop["path"],
                                                                     sample.name,
                                                                     self.work_dir,
                                                                     sample.seqs_num,
                                                                     sample.total_base_num,
                                                                     sample.seqs_mean_len,
                                                                     sample.seqs_min_len,
                                                                     sample.seqs_max_len))

    def return_sample(self, sample_name):
        if sample_name in self.samples:
            return self.samples[sample_name]
        fasta_dir = self.output_dir + "/fa"
        length_dir = self.output_dir + "/length"
        if not os.path.exists(fasta_dir):
            os.mkdir(fasta_dir)
        if not os.path.exists(length_dir):
            os.mkdir(length_dir)
        sample = Sample(sample_name, fasta_dir=fasta_dir, length_dir=length_dir)
        self.samples[sample_name] = sample
        return sample

    def run(self):
        super(FastqSampleExtractTool, self).run()
        self.parse_fastq(self.option("in_fastq").prop["path"])
        self.end()
