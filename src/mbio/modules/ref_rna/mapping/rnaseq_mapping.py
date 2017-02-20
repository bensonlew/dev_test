#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile
import glob
import shutil


class RnaseqMappingModule(Module):
    """
    refRNA的比对工具: tophat、hisat,并将用户上传的参考基因组gff文件转换为bed格式文件和gtf格式文件
    version 1.0
    author: sj
    last_modify: 2016.09.13
    """
    def __init__(self, work_id):
        super(RnaseqMappingModule, self).__init__(work_id)
        options = [
            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},
            # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "mapping_method", "type": "string"},  # 测序手段，分为tophat测序和hisat测序    
            {"name": "seq_method", "type": "string"},  # 双端测序还是单端测序
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # 单端序列
            {"name": "left_reads", "type": "infile", "format": "sequence.fastq"},  # 双端测序时，左端序列
            {"name": "right_reads", "type": "infile", "format": "sequence.fastq"},  # 双端测序时，右端序列
            {"name": "bam_output", "type": "outfile", "format": "ref_rna.assembly.bam_dir"},  # 输出的bam
            {"name": "assemble_method", "type": "string", "default": "None"},  # 拼接手段，None
            {"name": "mate_std", "type": "int", "default": 50},  # 末端配对插入片段长度标准差
            {"name": "mid_dis", "type": "int", "default": 50},  # 两个成对引物间的距离中间值
            {"name": "result_reserved", "type": "int", "default": 1}  # 最多保留的比对结果数目
        ]
        self.add_option(options)
        self.samples = {}
        self.tool_opts = {}
        self.tools = []
        
    def check_options(self):
        """
        检查参数
        """
        if self.option("fastq_dir").is_set:
            self.samples = self.get_list()
            # self.logger.info(self.samples)
            list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
            row_num = len(open(list_path, "r").readline().split())
            self.logger.info(row_num)
            if self.option('seq_method') == "PE" and row_num != 3:
                raise OptionError("PE序列list文件应该包括文件名、样本名和左右端说明三列")
            elif self.option('seq_method') == "SE" and row_num != 2:
                raise OptionError("SE序列list文件应该包括文件名、样本名两列")
        if self.option('seq_method') not in ['PE', 'SE']:
            raise OptionError("请说明序列类型，PE or SE?")
        if not self.option("fastq_dir").is_set and self.option('seq_method') in ["PE"]:
            if self.option("single_end_reads").is_set:
                raise OptionError("您上传的是单端测序的序列，请上传双端序列")
            elif not (self.option("left_reads").is_set and self.option("right_reads").is_set):
                raise OptionError("您漏了某端序列")
        if not self.option("fastq_dir").is_set and self.option('seq_method') == "SE":
            if not self.option("single_end_reads").is_set:
                raise OptionError("请上传单端序列")
            elif self.option("left_reads").is_set or self.option("right_reads").is_set:
                raise OptionError("有单端的序列就够啦")
        if not self.option("mapping_method") in ["tophat", "hisat"]:
            raise OptionError("tophat、hisat,选一个吧")
        if not self.option("assemble_method") in ["cufflinks", "stringtie", "None"]:
            raise OptionError("请选择拼接软件")
        return True
        
    def get_opts(self):
        self.tool_opts = {
            "ref_genome": self.option("ref_genome"),
            "mapping_method": self.option("mapping_method"),
            "seq_method": self.option("seq_method"),
            "assemble_method": self.option("assemble_method"),
        }
        return True
    
    def run(self):
        # super(RnaseqMappingModule,self).run()
        self.get_opts()
        if self.option("ref_genome") == "customer_mode":
            self.tool_opts.update({
                "ref_genome_custom": self.option("ref_genome_custom")
            })
        if self.option("mapping_method") == "tophat":
            self.logger.info("tophat开始运行")
            self.tool_run("tophat")
        elif self.option("mapping_method") == "hisat":
            self.logger.info("hisat开始运行")
            self.tool_run("hisat")
        else:
            self.set_error("比对软件选择错误")
            raise Exception("比对软件选择错误,程序退出")
        super(RnaseqMappingModule, self).run()
        
    def tool_run(self, tool):
        if self.option("seq_method") == "PE":
            for f in self.samples:
                fq_l = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                fq_r = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                mapping_tool = self.add_tool('ref_rna.mapping.' + tool)
                self.tool_opts.update({
                    'left_reads': fq_l,
                    'right_reads': fq_r,
                    'sample': f
                })
                if tool == "tophat":
                    self.tool_opts.update({
                        "mate_std": self.option("mate_std"),
                        "mid_dis": self.option("mid_dis"),
                        "result_reserved": self.option("result_reserved")
                    })
                mapping_tool.set_options(self.tool_opts)
                self.tools.append(mapping_tool)

        else:
            for f in self.samples:
                fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                mapping_tool = self.add_tool('ref_rna.mapping.' + tool)
                self.tool_opts.update({
                    'single_end_reads': fq_s,
                    'sample': f
                })
                if tool == "tophat":
                    self.tool_opts.update({
                        "mate_std": self.option("mate_std"),
                        "mid_dis": self.option("mid_dis"),
                        "result_reserved": self.option("result_reserved")
                    })
                mapping_tool.set_options(self.tool_opts)
                self.tools.append(mapping_tool)
        self.on_rely(self.tools, self.set_output)
        # self.on_rely(self.tools, self.end)
        for tool in self.tools:
            tool.run()
            
    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples
        
    def end(self):
        super(RnaseqMappingModule, self).end()
        
    def set_output(self):
        self.logger.info("set output")
        for f in glob.glob(r"{}/*".format(self.output_dir)):
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.remove(f)
        for tool in self.tools:
            out_files = os.listdir(tool.output_dir)
            for f in out_files:
                f_path = os.path.join(tool.output_dir, f)
                target = os.path.join(self.output_dir, f)
                if os.path.exists(target):
                    os.remove(target)
                os.link(f_path, target)
        self.option("bam_output").set_path(self.output_dir)
        self.logger.info("done")
        self.end()
