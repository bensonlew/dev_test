#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import glob
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile
import json

class SnpRnaModule(Module):
    """
    star:比对
    picard:处理sam文件
    gatk：snp calling软件
    version 1.0
    author: chenyanyan
    last_modify: 2016.09.28
    """
    def __init__(self, work_id):
        super(SnpRnaModule, self).__init__(work_id)
        self._ref_genome_lst = ["customer_mode","Chicken","Tilapia","Zebrafish","Cow","Pig","Fruitfly","Human","Mouse","Rat","Arabidopsis","Broomcorn",\
        "Rice","Zeamays", "Test"]
        options = [
            {"name": "ref_genome", "type": "string"}, #参考基因组类型
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"}, #自定义参考基因组文件 
            {"name":"readFilesIN", "type":"infile", "format":"sequence.fastq"},#用于比对的单端序列文件
            {"name":"readFilesIN1", "type":"infile", "format":"sequence.fastq"},#单端序列
            {"name":"readFilesIN2", "type":"infile", "format":"sequence.fastq"},#单端序列
            {"name": "in_sam", "type": "infile", "format": "align.bwa.sam"},  # sam格式文件
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},#用于比对的文件夹
            {"name":"seq_method", "type": "string"},#比对方式
            {"name": "input_bam", "type": "infile", "format": "align.bwa.bam"} # bam格式文件,排序过的
        ]
        self.add_option(options)
        self.samples = {}  #样本 字典 用途？？？多个样本的情况
        self.fastq_files = []
        self.end_info = 1
        self.mapping_tools = [] #star的运行步骤
        self.picard = []
        self.gatk = []
        self.ref_link = ""
        self.step.add_steps('star')
        self.end_times = 1
        
    def check_options(self):
        """
        检查参数
        """
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请选择参考基因组类型！")
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请传入自定义参考序列!")
        #if not self.option("in_sam").is_set:
        #   raise OptionError("请提供sam输入文件")
        #if not self.option("input_bam").is_set:
        #   raise OptionError("请提供bam输入文件")

            
    def star_finish_update(self):
        self.star.step_end()
        self.step.update()

    def picard_finish_update(self):
        self.picard.step_end()
        self.step.update()

    def gatk_finish_update(self):
        self.gatk.step_end()
        self.step.update()
        
    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()
        
    def star_multi_run(self):
        #self.fastq_files = []
        n = 0
        self.logger.info("1234")
        self.samples = self.get_list()
        if self.option("ref_genome") != "customer_mode": #不是自定义模式,本地数据库的参考基因组
            self.ref_name = self.option("ref_genome")
            self.logger.info("5678")
            if self.option("seq_method") == "PE": 
                self.logger.info("666")
                for f in self.samples:
                    fq1 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq2 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"]) 
                    self.logger.info("fq1")
                    star = self.add_tool('ref_rna.gene_structure.star')
                    self.step.add_steps('star_{}'.format(n))
                    star.set_options({
                        "ref_genome": self.ref_name,
                        "readFilesIN1": fq1,
                        "readFilesIN2": fq2,
                        "seq_method": self.option("seq_method") 
                    })
                    
                    step = getattr(self.step, 'star_{}'.format(n))
                    step.start()
                    star.on("end", self.finish_update, "star_{}".format(n))
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
                    
            elif self.option("seq_method") == "SE":
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    self.step.add_steps('star_{}'.format(n))
                    star = self.add_tool('ref_rna.gene_structure.star')
                    self.step.add_steps('star_{}'.format(n))
                    star.set_options({
                        "ref_genome": self.ref_name,
                        "readFilesIN": fq_s,
                        "seq_method": self.option("seq_method")
                    })
                    step = getattr(self.step, 'star_{}'.format(n))
                    step.start()
                    star.on("end", self.finish_update, "star_{}".format(n))
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
        
        else: #用户上传基因组
            ref_fasta = self.option('ref_genome_custom').prop["path"] #用户上传的基因组路径
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link)  # 将参考基因组链接到self.work_dir下
            
            if self.option("seq_method") == "PE":  # 如果测序方式为PE测序
                for f in self.samples:
                    fq1 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq2 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    self.step.add_steps('star_{}'.format(n))
                    star.set_options({
                        "ref_genome":"customer_mode",
                        "ref_genome_custom": self.ref_link,
                        "readFilesIN1": fq1,
                        "readFilesIN2": fq2,
                        'seq_method': self.option('seq_method'),
                        
                    })
                    step = getattr(self.step, 'star_{}'.format(n))
                    step.start()
                    star.on("end", self.finish_update, "star_{}".format(n))
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
            elif self.option("seq_method") == "SE":  # 如果测序方式为SE测序
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    self.step.add_steps('star_{}'.format(n))
                    star.set_options({
                        "ref_genome":"customer_mode",
                        "ref_genome_custom": self.ref_link,
                        'readFilesIN': fq_s,
                        'seq_method': self.option('seq_method'),
                    })
                    step = getattr(self.step, 'star_{}'.format(n))
                    step.start()
                    star.on("end", self.finish_update, "star_{}".format(n))
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
        #self.on_rely(self.mapping_tools, self.end)
        
        if len(self.mapping_tools) == 1:
            # self.mapping_tools[0].on("end", self.multi_samtools_run)
            self.mapping_tools[0].run()
        else:
            for tool in self.mapping_tools:
                tool.run()
    
    def star_single_run(self):
        self.star = self.add_tool('ref_rna.gene_structure.star')
        if self.option("ref_genome") in ["customer_mode"]:  
            if self.option("seq_method") == "PE":
                self.star.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'readFilesIN1': self.option('readFilesIN1').prop["path"],
                    'readFilesIN2': self.option('readFilesIN2').prop["path"],
                    'seq_method': self.option('seq_method')
                })
            elif self.option("seq_method") == "SE":
                self.star.set_options({
                    "ref_genome":"customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'readFilesIN': self.option('readFilesIN').prop["path"],
                    'seq_method': self.option('seq_method')
                })
                    
        else:  # 本地参考基因组
            if self.option("seq_method") == "PE":  # 双端测序
                self.star.set_options({
                    "ref_genome": self.option("ref_genome"),
                    'readFilesIN1': self.option('readFilesIN1').prop["path"],
                    'readFilesIN2': self.option('readFilesIN2').prop["path"],
                    'seq_method': self.option('seq_method')
                })
            elif self.option("seq_method") == "SE":  # 单端测序
                self.star.set_options({
                    "ref_genome":self.option("ref_genome"),
                    'readFilesIN': self.option('readFilesIN').prop["path"],
                    'seq_method': self.option('seq_method')
                })
        
        self.star.step_start() # 错误信息AttributeError: 'StarAgent' object has no attribute 'step_start'
        self.star.on("end", self.star_finish_update)
        self.star.on("end", self.picard_run)
        self.star.run()
    
    def picard_run(self, event):
        obj = event["bind_object"]
        star_output = os.listdir(obj.output_dir)
        f_path = os.path.join(obj.output_dir, star_output[0])
        self.logger.info(f_path) #打印出f_path的信息，是上一步输出文件的路径
        picard = self.add_tool('ref_rna.gene_structure.picard_rna')
        self.step.add_steps('picard_{}'.format(event['data']))
        if self.option("ref_genome") == "customer_mode":
            ref_fasta = self.option('ref_genome_custom').prop["path"] #用户上传的基因组路径
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)#链接到当前工作目录下
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link) 
            picard.set_options({
                "ref_genome_custom": self.ref_link,
                "in_sam": f_path,
                "ref_genome": self.option("ref_genome")
            })
            step = getattr(self.step, 'picard_{}'.format(event["data"]))
            step.start()
            picard.on("end", self.finish_update, 'picard_{}'.format(event["data"]))
            picard.on("end", self.gatk_run)
            self.logger.info("picard is processing")
            #picard.run()
            self.picard.append(picard)
            
        else:
            self.ref_name = self.option("ref_genome")
            picard.set_options({
                "ref_genome": self.ref_name,
                "in_sam": f_path
            })
            step = getattr(self.step, 'picard_{}'.format(event["data"]))
            step.start()
            picard.on("end", self.finish_update, 'picard_{}'.format(event["data"]))
            picard.on("end", self.gatk_run)
            self.logger.info("picard is processing")
            #picard.run()
            self.picard.append(picard)

        #self.on_rely(self.picard, self.end)
        
        if len(self.picard) == 1:
            # self.mapping_tools[0].on("end", self.multi_samtools_run)
            self.picard[0].run()
        else:
            for tool in self.picard:
                tool.run()
        
    def gatk_run(self, event): 
        obj = event["bind_object"]
        picard_output = os.listdir(obj.output_dir)
        for i in picard_output:
            if i.endswith(".bam"):
                f_path = os.path.join(obj.output_dir, i)
                self.logger.info(f_path)
        gatk = self.add_tool('ref_rna.gene_structure.gatk')
        self.step.add_steps('gatk_{}'.format(event['data']))
        if self.option("ref_genome") == "customer_mode":
            ref_fasta = self.option('ref_genome_custom').prop["path"] #用户上传的基因组路径
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link) 
            gatk.set_options({
                "ref_fa": self.ref_link,
                "input_bam": f_path,
                "ref_genome": "customer_mode"
            })
            step = getattr(self.step, 'gatk_{}'.format(event["data"]))
            step.start()
            gatk.on("end", self.finish_update, 'gatk_{}'.format(event["data"]))
            gatk.on("end", self.set_output)
            self.logger.info("gatk is running!")
            #gatk.run()
            self.gatk.append(gatk)
            
        else:
            self.ref_name = self.option("ref_genome")
            gatk.set_options({
                "ref_genome": self.ref_name,
                "input_bam": f_path
            })
            step = getattr(self.step, 'gatk_{}'.format(event["data"]))
            step.start()
            gatk.on("end", self.finish_update, 'gatk_{}'.format(event["data"]))
            gatk.on("end", self.set_output)
            self.logger.info("gatk is running!")
            #gatk.run()
            self.gatk.append(gatk)

        #self.on_rely(self.gatk, self.end)
        
        if len(self.gatk) == 1:
            # self.mapping_tools[0].on("end", self.multi_samtools_run)
            self.gatk[0].run()
        else:
            for tool in self.gatk:
                tool.run()

    def rename(self, event):
        obj = event["bind_object"]
        self.logger.info("obj is "+ str(obj))
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            self.logger.info("lalala")  
        self.end() # rename没看明白
    
    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples
    
    def set_output(self):
        self.logger.info("set output")
        if self.end_times < len(self.samples):
            self.end_times += 1
        elif self.end_times == len(self.samples):
            for f in os.listdir(self.output_dir):
                f_path = os.path.join(self.output_dir, f)
                if os.path.isdir(f_path):
                    shutil.rmtree(f_path)
                else:
                    os.remove(f_path)
            vcf_dir = os.path.join(self.output_dir, "vcf_files")
           
            if not os.path.exists(vcf_dir):
                os.makedirs(vcf_dir)
            else:
                for f in os.listdir(vcf_dir):
                    f_path = os.path.join(vcf_dir, f)
                    if os.path.isdir(f_path):
                        shutil.rmtree(f_path)
                    else:
                        os.remove(f_path)
            for f in os.listdir(self.output_dir):
                if not os.path.isdir(f):
                    f = os.path.join(self.output_dir, f)
                    self.logger.info("move f")
                    shutil.move(f, vcf_dir)
        self.end()
   
    def run(self):
        if self.option("fastq_dir").is_set:
            self.star_multi_run()
            self.logger.info("star multi finished!")
       
        else:
            self.star_single_run()
            self.logger.info("star single finished!")
        super(SnpRnaModule, self).run()


    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            [r"./filtered_vcf/", "文件夹", "过滤后的vcf格式的SNP位点文件结果输出目录"]
        ])
        super(SnpRnaModule, self).end()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            