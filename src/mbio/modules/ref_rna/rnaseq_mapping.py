#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile


class RnaseqMappingModule(Module):
    """
    refRNA的比对工具: tophat、hisat,并将用户上传的参考基因组gff文件转换为bed格式文件和gtf格式文件
    version 1.0
    author: sj
    last_modify: 2016.09.13
    """
    def __init__(self, work_id):
        super(RnaseqMappingModule, self).__init__(work_id)
        self._ref_genome_lst = ["customer_mode","Chicken","Tilapia","Zebrafish","Cow","Pig","Fruitfly","Human","Mouse","Rat","Arabidopsis","Broomcorn",\
        "Rice","Zeamays","Test"]
        options = [
            {"name": "ref_genome", "type": "string"},  # 参考基因组，在页面上呈现为下拉菜单中的选项
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组，用户选择customer_mode时，需要传入参考基因组
            {"name": "mapping_method", "type": "string"},  # 测序手段，分为tophat测序和hisat测序    
            {"name":"seq_method", "type": "string"},  # 双端测序还是单端测序
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "single_end_reads", "type": "infile", "format": "sequence.fastq"},  # 单端序列
            {"name": "left_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，左端序列
            {"name": "right_reads", "type": "infile", "format":"sequence.fastq"},  # 双端测序时，右端序列
            {"name": "gff","type": "infile", "format":"ref_rna.reads_mapping.gff"},  # gff格式文件
            {"name": "bam_output", "type": "outfile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # 输出的bam
            #{"name": "gtf", "type": "outfile", "format" : "ref_rna.reads_mapping.gtf"},  # 输出的gtf格式文件
            #{"name": "bed", "type": "outfile", "format" : "ref_rna.reads_mapping.bed"},  # 输出的bed格式文件
            {"name": "assemble_method", "type": "string"}
        ]
        self.add_option(options)
        self.samples = {}
        self.samtools = []
        self.mapping_tools = []
        self.bwa = None
        self.end_times = 1
        self.ref_link = ""
        self.ref_name = ""
        self.ref_fasta = ""

    def check_options(self):
        """
        检查参数
        """
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请选择参考序列")
        if self.option("fastq_dir").is_set:
            self.samples = self.get_list()
            self.logger.info(self.samples)
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
            elif not (self.option("left_reads").is_set and  self.option("right_reads").is_set):
                raise OptionError("您漏了某端序列")
        if not self.option("fastq_dir").is_set and self.option('seq_method') == "SE":
            if not self.option("single_end_reads").is_set:
                raise OptionError("请上传单端序列")
            elif self.option("left_reads").is_set or  self.option("right_reads").is_set:
                raise OptionError("有单端的序列就够啦")
        if not self.option("mapping_method") in ["tophat","hisat"]:
            raise OptionError("tophat、hisat,选一个吧")
        if not self.option("assemble_method") in ["cufflinks","stringtie","None"]:
            raise OptionError("请选择拼接软件")
        return True

    def tophat_finish_update(self):
        self.tophat.step_end()
        self.step.update()

    def hisat_finish_update(self):
        self.hisat.step_end()
        self.step.update()

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()


    def multi_tophat_run(self):
        n = 0
        if self.option("ref_genome") not in ["customer_mode"]: # 如果是本地参考基因组
            self.ref_name = self.option("ref_genome")
            if self.option("seq_method") == "PE":  # 如果是双端测序
                for f in self.samples:
                    fq_l = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq_r = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    tophat = self.add_tool('ref_rna.mapping.tophat')
                    self.step.add_steps('tophat_{}'.format(n))
                    tophat.set_options({
                        "ref_genome": self.ref_name,
                        'left_reads': fq_l,
                        'right_reads': fq_r,
                        'seq_method': self.option("seq_method"),
                        "mapping_method":"tophat"
                    })
                    step = getattr(self.step, 'tophat_{}'.format(n))
                    step.start()
                    tophat.on("end", self.finish_update, "tophat_{}".format(n))
                    self.mapping_tools.append(tophat)
            elif self.option("seq_method") == "SE":  # 如果是单端测序
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    tophat = self.add_tool('ref_rna.mapping.tophat')
                    self.step.add_steps('tophat_{}'.format(n))
                    tophat.set_options({
                        "ref_genome":self.ref_name,
                        'single_end_reads': fq_s,
                        'seq_method': self.option('seq_method'),
                        "mapping_method":"tophat"
                    })
                    step = getattr(self.step, 'tophat_{}'.format(n))
                    step.start()
                    tophat.on("end", self.finish_update, "tophat_{}".format(n))
                    self.mapping_tools.append(tophat)
        else:  # 如果是用户上传的基因组
            ref_fasta = self.option('ref_genome_custom').prop["path"]
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link)  # 将参考基因组链接到self.work_dir下
            if self.option("seq_method") == "PE":  # 如果测序方式为PE测序
                for f in self.samples:
                    fq_l = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq_r = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    tophat = self.add_tool('ref_rna.mapping.tophat')
                    self.step.add_steps('tophat_{}'.format(n))
                    tophat.set_options({
                        "ref_genome":"customer_mode",
                        "ref_genome_custom": self.ref_link,
                        'left_reads': fq_l,
                        'right_reads': fq_r,
                        'seq_method': self.option('seq_method'),
                        "mapping_method":"tophat"
                    })
                    step = getattr(self.step, 'tophat_{}'.format(n))
                    step.start()    
                    tophat.on("end", self.finish_update, "tophat_{}".format(n))
                    self.mapping_tools.append(tophat)
            elif self.option("seq_method") == "SE":  # 如果测序方式为SE测序
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    tophat = self.add_tool('ref_rna.mapping.tophat')
                    self.step.add_steps('tophat_{}'.format(n))
                    tophat.set_options({
                        "ref_genome":"customer_mode",
                        "ref_genome_custom": self.ref_link,
                        'single_end_reads': fq_s,
                        'seq_method': self.option('seq_method'),
                        "mapping_method":"tophat"
                    })
                    step = getattr(self.step, 'tophat_{}'.format(n))
                    step.start()
                    tophat.on("end", self.finish_update, "tophat_{}".format(n))
                    self.mapping_tools.append(tophat)
        self.on_rely(self.mapping_tools, self.set_output)
        if len(self.mapping_tools) == 1:
            self.mapping_tools[0].run()
        else:
            for tool in self.mapping_tools:
                tool.run()

    def tophat_single_run(self):
        self.tophat = self.add_tool('ref_rna.mapping.tophat')
        if self.option("ref_genome") in ["customer_mode"]:  
            if self.option("seq_method") == "PE":
                self.tophat.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'left_reads': self.option('left_reads').prop["path"],
                    'right_reads': self.option('right_reads').prop["path"],
                    'seq_method': self.option('seq_method')
                    })
            elif self.option("seq_method") == "SE":
                self.tophat.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'single_end_reads': self.option('single_end_reads').prop["path"],
                    'seq_method': self.option('seq_method')
                    })
        else:  # 本地参考基因组
            if self.option("seq_method") == "PE":  # 双端测序
                self.tophat.set_options({
                    "ref_genome":self.option("ref_genome"),
                    'left_reads': self.option('left_reads').prop["path"],
                    'right_reads': self.option('right_reads').prop["path"],
                    'seq_method': self.option('seq_method')
                    })
            elif self.option("seq_method") == "SE":  # 单端测序
                self.tophat.set_options({
                    "ref_genome":self.option("ref_genome"),
                    'single_end_reads': self.option('single_end_reads').prop["path"],
                    'seq_method': self.option('seq_method')
                    })
        self.tophat.step_start()
        self.tophat.on("end", self.tophat_finish_update)
        self.tophat.on("end", self.end)
        self.tophat.run()

    def multi_hisat_run(self):
        n = 0
        if self.option("ref_genome") not in ["customer_mode"]:
            self.ref_name = self.option("ref_genome")
            if self.option("seq_method") == "PE":
                for f in self.samples:
                    fq_l = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq_r = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    hisat = self.add_tool('ref_rna.mapping.hisat')
                    self.step.add_steps('hisat_{}'.format(n))
                    hisat.set_options({
                        "ref_genome": self.ref_name,
                        'left_reads': fq_l,
                        'right_reads': fq_r,
                        'seq_method': self.option('seq_method'),
                        "assemble_method":self.option("assemble_method")
                    })
                    step = getattr(self.step, 'hisat_{}'.format(n))
                    step.start()
                    hisat.on("end", self.finish_update, "hisat_{}".format(n))
                    hisat.on("end", self.set_output)
                    self.mapping_tools.append(hisat)
            elif self.option("seq_method") == "SE":
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    hisat = self.add_tool('ref_rna.mapping.hisat')
                    self.step.add_steps('hisat_{}'.format(n))
                    hisat.set_options({
                        "ref_genome":self.ref_name,
                        'single_end_reads': fq_s,
                        'seq_method': self.option('seq_method'),
                        'assemble_method':self.option("assemble_method")
                    })
                    step = getattr(self.step, 'hisat_{}'.format(n))
                    step.start()
                    hisat.on("end", self.finish_update, "hisat_{}".format(n))
                    hisat.on("end", self.set_output)
                    self.mapping_tools.append(hisat)
        else:  # 用户自定义模式，自己上传基因组
            self.ref_name = "customer_mode"
            ref_fasta = self.option('ref_genome_custom').prop["path"]
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link)
            if self.option("seq_method") == "PE":  # hisat双端测序
                for f in self.samples:
                    fq_l = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq_r = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    hisat = self.add_tool('ref_rna.mapping.hisat')
                    self.step.add_steps('hisat_{}'.format(n))
                    hisat.set_options({
                        "ref_genome": self.ref_name,
                        "ref_genome_custom":self.ref_link,
                        'left_reads': fq_l,
                        'right_reads': fq_r,
                        'seq_method': self.option('seq_method'),
                        "assemble_method":self.option("assemble_method")
                    })
                    step = getattr(self.step, 'hisat_{}'.format(n))
                    step.start()
                    hisat.on("end", self.finish_update, "hisat_{}".format(n))
                    hisat.on("end", self.set_output)
                    self.mapping_tools.append(hisat)
            elif self.option("seq_method") == "SE":  # hisat单端测序
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    hisat = self.add_tool('ref_rna.mapping.hisat')
                    self.step.add_steps('hisat_{}'.format(n))
                    hisat.set_options({
                        "ref_genome":self.ref_name,
                        "ref_genome_custom":self.ref_link,
                        'single_end_reads': fq_s,
                        'seq_method': self.option('seq_method'),
                        'assemble_method':self.option("assemble_method")
                    })
                    step = getattr(self.step, 'hisat_{}'.format(n))
                    step.start()
                    hisat.on("end", self.finish_update, "hisat_{}".format(n))
                    hisat.on("end", self.set_output)
                    self.mapping_tools.append(hisat)  
        self.on_rely(self.mapping_tools, self.end)
        if len(self.mapping_tools) == 1:
            self.mapping_tools[0].run()
        else:
            for tool in self.mapping_tools:
                tool.run()

    def hisat_single_run(self):
        self.hisat = self.add_tool('ref_rna.mapping.hisat')
        if self.option("ref_genome") in ["customer_mode"]:
            if self.option("seq_method") == "PE":
                self.hisat.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'left_reads': self.option('left_reads').prop["path"],
                    'right_reads': self.option('right_reads').prop["path"],
                    'seq_method': self.option('seq_method'),
                    'assemble_method':self.option("assemble_method")
                    })
            elif self.option("seq_method") == "SE":
                self.hisat.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'single_end_reads': self.option('single_end_reads').prop["path"],
                    'seq_method': self.option('seq_method'),
                    'assemble_method':self.option("assemble_method")
                    })
        else:  # 用户上传基因组
            if self.option("seq_method") == "PE":
                self.hisat.set_options({
                    "ref_genome":self.option("ref_genome"),
                    'left_reads': self.option('left_reads').prop["path"],
                    'right_reads': self.option('right_reads').prop["path"],
                    'seq_method': self.option('seq_method'),
                    'assemble_method':self.option("assemble_method")
                    })
            elif self.option("seq_method") == "SE":
                self.hisat.set_options({
                    "ref_genome":self.option("ref_genome"),
                    'single_end_reads': self.option('single_end_reads').prop["path"],
                    'seq_method': self.option('seq_method'),
                    'assemble_method':self.option("assemble_method")
                    })
        self.hisat.step_start()
        self.hisat.on("end", self.hisat_finish_update)
        self.hisat.on("end",self.set_output) 
        self.hisat.on("end",self.end)
        self.hisat.run()

    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples

    def set_output(self):
        self.logger.info("set output")
        for f in glob.glob(r"{}/*".format(self.output_dir)):
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.remove(f)
        for tool in self.mapping_tools:
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
        
            
    def run(self):
        if self.option("fastq_dir").is_set:
            if self.option("mapping_method") == "tophat":
                self.multi_tophat_run()
            elif self.option("mapping_method") == "hisat":
                self.multi_hisat_run()
        else:
            if self.option("mapping_method") == "tophat":
                self.tophat_single_run()    
            elif self.option("mapping_method") == "hisat":
                self.hisat_single_run()
        super(RnaseqMappingModule, self).run()

    def end(self):
        """
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            [r"./sorted_bam/", "文件夹", "排序后的bam格式比对结果文件输出目录"]
        ])
        """
        super(RnaseqMappingModule, self).end()
