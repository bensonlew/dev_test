# -*- coding: utf-8 -*-
# __author__ = 'sj'

from __future__ import division
import os,re
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError

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
            {"name": "length_dir", "type":"outfile","format" : "sequence.length_dir"}  # 新增length文件夹文件格式,之后进行修改
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
        self._memory = "4G"  # 内存应该设大一些

class FastqSampleExtractTool(Tool):
    def __init__(self, config):
        super(FastqSampleExtractTool, self).__init__(config)

    def create_file_sample(self):
        path = os.path.join(self.output_dir, "list.txt")
        
        with open(self.option("in_fastq").prop["path"],"r") as r:
            # w.write("{}\t{}\n".format(self.option("in_fastq").prop["path"], sp))
            sample_list = []  #  样本名称列表
            seq_num = {}  # 样本序列数目
            base_num = {}  # 样本碱基总数
            min_length = {}  # 样本最小长度
            max_length = {}  # 样本最大长度
            os.mkdir(self.output_dir + "/" + "length")
            os.mkdir(self.output_dir + "/" + "fa")
            fa_path = self.output_dir + "/" + "fa"
            for line in r:
                m = re.match("@(.+)_(\d+)",line)
                if m:
                    sample_name = m.group(1)  # 样本名称
                    seq = m.group(2)    # 获得序列名称
                    # seq = other_thing.split("\s")[0]  # 获得序列名称
                    if sample_name not in sample_list:
                        if len(sample_list) != 0:
                            sample = sample_list[-1]
                            seq_num[sample] = seq_tmp
                            base_num[sample] = sum(sample_length_tmp)
                            min_length[sample] = min
                            max_length[sample] = max
                            
                        sample_list.append(sample_name)  # 将样本名称加入列表中
                        sample_length_tmp = []  # 用来储存样本序列长度的临时列表
                        seq_tmp = 0  # 用来储存样本序列数的临时变量
                        min = 0
                        max = 0
                        i = 0 
                        seq_path = os.path.join(fa_path,sample_name + ".fasta")  
                    length_path = os.path.join(self.output_dir + "/length",sample_name + ".length_file")
                    seq_tmp += 1
                    with open(seq_path,"a") as a:
                        with open(length_path,"a") as l:
                            a.write(">" + seq + "\n")
                            line2 = r.next()
                            lth = len(line2) - 1
                            sample_length_tmp.append(lth)
                            if i == 0:
                                min = lth
                                i += 1
                            elif lth < min:  
                                min = lth
                            elif lth > max:
                                max = lth
                            a.write(line2)
                            l.write(str(lth) + "\n")
                            r.next()
                            r.next()
                else:
                    print "什么鬼，不匹配"
            sample = sample_list[-1]
            seq_num[sample] = seq_tmp
            base_num[sample] = sum(sample_length_tmp)
            min_length[sample] = min
            max_length[sample] = max
            info_path = os.path.join(self.work_dir,"info.txt")
            with open(info_path,"w") as w:
                w.write("#file_path\tsample\twork_dir_path\tseq_num\tbase_num\tmean_length\tmin_length\tmax_length\n")
                for sample in sample_list:
                    w.write(self.option("in_fastq").prop["path"]+ "\t" + sample +"\t"+self.work_dir + "\t" + str(seq_num[sample])\
                    + "\t" + str(base_num[sample]) + "\t" + str(base_num[sample] / seq_num[sample]) + "\t"\
                    + str(min_length[sample]) + "\t" + str(max_length[sample]) + "\n")
        self.option("file_sample_list").set_path(info_path)
        self.option("length_dir").set_path(length_path)
        self.option("out_fa").set_path(fa_path)

    def run(self):
        super(FastqSampleExtractTool, self).run()
        self.create_file_sample()
        self.end()
        
