#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
# import glob
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.files.sequence.file_sample import FileSampleFile
# import json
# import re


class SnpRnaModule(Module):
    """
    star:序列比对
    picard:处理比对结果sam文件
    gatk：snp calling软件
    version 1.0
    author: chenyanyan
    last_modify: 2017.01.03 by qindanhua
    """
    def __init__(self, work_id):
        super(SnpRnaModule, self).__init__(work_id)
        self._ref_genome_lst = ["customer_mode", "Chicken", "Tilapia", "Zebrafish", "Cow", "Pig", "Fruitfly", "Human",
                                "Mouse", "Rat", "Arabidopsis", "Broomcorn", "Rice", "Zeamays", "Test"]
        options = [
            {"name": "ref_genome", "type": "string"},  # 参考基因组类型
            {"name": "ref_genome_custom", "type": "infile", "format": "sequence.fasta"},  # 自定义参考基因组文件
            {"name": "readFilesIN", "type": "infile", "format": "sequence.fastq"},  # 用于比对的单端序列文件
            {"name": "readFilesIN1", "type": "infile", "format":"sequence.fastq, sequence.fasta"},  # 双端序列←
            {"name": "readFilesIN2", "type": "infile", "format":"sequence.fastq, sequence.fasta"},  # 双端序列右
            {"name": "in_sam", "type": "infile", "format": "align.bwa.sam"},  # sam格式文件
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # 用于比对的文件夹
            {"name": "seq_method", "type": "string"},  # 比对方式
            {"name": "input_bam", "type": "infile", "format": "align.bwa.bam"}  # bam格式文件,排序过的
        ]
        self.add_option(options)
        self.samples = {}  
        self.fastq_files = []
        self.mapping_tools = []  # star的tools
        self.picards = []
        self.gatks = []
        self.step.add_steps('star', 'picard', 'gatk')  # 添加步骤
        self.count = 0
        self.ref_name = ""
        self.annovars = []      # add by qindanhua

    def check_options(self):
        """
        检查参数
        """
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请选择参考基因组类型！")
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请传入自定义参考序列!")

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()
        
    def star_multi_run(self):
        self.fastq_files = []
        self.samples = self.get_list()  # 获取多个输入文件夹的序列路径
        self.logger.info(len(self.samples))
        if self.option("ref_genome") != "customer_mode":  # 本地数据库的参考基因组
            self.ref_name = self.option("ref_genome")
            if self.option("seq_method") == "PE": 
                for f in self.samples:
                    fq1 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq2 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"]) 
                    star = self.add_tool('ref_rna.gene_structure.star')  # add_tool(self, path) return agent
                    star.set_options({
                        "ref_genome": self.ref_name,
                        "readFilesIN1": fq1,
                        "readFilesIN2": fq2,
                        "seq_method": self.option("seq_method") 
                    })   # set_options(options)方法在 tool.py里 options需为一个字典对对象
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
                    
            elif self.option("seq_method") == "SE":
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    star.set_options({
                        "ref_genome": self.ref_name,
                        "readFilesIN": fq_s,
                        "seq_method": self.option("seq_method")
                    })
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)
        
        else:  # 用户上传基因组
            ref_fasta = self.option('ref_genome_custom').prop["path"]  # 用户上传的基因组路径
            self.ref_link = self.work_dir + "/" + os.path.basename(ref_fasta)
            if os.path.exists(self.ref_link):
                os.remove(self.ref_link)
            os.link(ref_fasta, self.ref_link)  # 将参考基因组链接到self.work_dir下
            
            if self.option("seq_method") == "PE":  # 如果测序方式为PE测序
                for f in self.samples:
                    fq1 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["l"])
                    fq2 = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f]["r"])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    star.set_options({
                        "ref_genome": "customer_mode",
                        "ref_genome_custom": ref_fasta,
                        "readFilesIN1": fq1,
                        "readFilesIN2": fq2,
                        'seq_method': self.option('seq_method')
                    })
                 
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)

            elif self.option("seq_method") == "SE":  # 如果测序方式为SE测序
                for f in self.samples:
                    fq_s = os.path.join(self.option('fastq_dir').prop["path"], self.samples[f])
                    star = self.add_tool('ref_rna.gene_structure.star')
                    star.set_options({
                        "ref_genome": "customer_mode",
                        "ref_genome_custom": ref_fasta,
                        'readFilesIN': fq_s,
                        'seq_method': self.option('seq_method')
                    })
                   
                    star.on("end", self.picard_run)
                    self.mapping_tools.append(star)

        self.on_rely(self.mapping_tools, self.finish_update, 'star')
        for tool in self.mapping_tools:
            tool.run()

    def star_single_run(self):
        star = self.add_tool('ref_rna.gene_structure.star')
        if self.option("ref_genome") in ["customer_mode"]:  
            if self.option("seq_method") == "PE":
                star.set_options({
                    "ref_genome": "customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'readFilesIN1': self.option('readFilesIN1').prop["path"],
                    'readFilesIN2': self.option('readFilesIN2').prop["path"],
                    'seq_method': self.option('seq_method')
                })
            elif self.option("seq_method") == "SE":
                star.set_options({
                    "ref_genome": "customer_mode",
                    "ref_genome_custom": self.option('ref_genome_custom').prop["path"],
                    'readFilesIN': self.option('readFilesIN').prop["path"],
                    'seq_method': self.option('seq_method')
                })
                    
        else:  # 本地参考基因组
            if self.option("seq_method") == "PE":  # 双端测序
                star.set_options({
                    "ref_genome": self.option("ref_genome"),
                    'readFilesIN1': self.option('readFilesIN1').prop["path"],
                    'readFilesIN2': self.option('readFilesIN2').prop["path"],
                    'seq_method': self.option('seq_method')
                })
            elif self.option("seq_method") == "SE":  # 单端测序
                star.set_options({
                    "ref_genome": self.option("ref_genome"),
                    'readFilesIN': self.option('readFilesIN').prop["path"],
                    'seq_method': self.option('seq_method')
                })
         
        star.on("end", self.finish_update, 'star')
        star.on("end", self.picard_run)
        star.run()
    
    def picard_run(self, event):
        obj = event["bind_object"]
        star_output = os.listdir(obj.output_dir)
        f_path = os.path.join(obj.output_dir, star_output[0])
        self.logger.info(f_path)  # 打印出f_path的信息，是上一步输出文件的路径
        picard = self.add_tool('ref_rna.gene_structure.picard_rna')
        self.picards.append(picard)
        self.logger.info(len(self.picards))
        if self.option("ref_genome") == "customer_mode":
            ref_fasta = self.option('ref_genome_custom').prop["path"]  # 用户上传的基因组路径
            picard.set_options({
                "ref_genome_custom": ref_fasta,
                "in_sam": f_path,
                "ref_genome": self.option("ref_genome")
            })
        else:
            self.ref_name = self.option("ref_genome")
            picard.set_options({
                "ref_genome": self.ref_name,
                "in_sam": f_path
            })
        picard.on("end", self.gatk_run)
        if len(self.picards) == 1 and self.samples is None:
            self.picards[0].on("end", self.finish_update, "picard")
            
        if len(self.picards) == len(self.samples):
            # if len(self.picards) == 1:
            #    self.picards[0].on('end', self.finish_update, 'picard')
            # else:
            self.on_rely(self.picards, self.finish_update, 'picard')
        picard.run()  # 全部tool运行完成后更新信息
        
    def gatk_run(self, event): 
        obj = event["bind_object"]
        picard_output = os.listdir(obj.output_dir)
        f_path = ""
        for i in picard_output:
            if i.endswith(".bam"):
                f_path = os.path.join(obj.output_dir, i)
                self.logger.info(f_path)
        gatk = self.add_tool('ref_rna.gene_structure.gatk')
        self.gatks.append(gatk)
        if self.option("ref_genome") == "customer_mode":
            ref_fasta = self.option('ref_genome_custom').prop["path"]  # 用户上传的基因组路径
            gatk.set_options({
                "ref_fa": ref_fasta,
                "input_bam": f_path,
                "ref_genome": "customer_mode"
            })
            # gatk.on("end", self.finish_update, 'gatk')
            # gatk.on("end", self.set_output)
            # self.logger.info("gatk is running!")
            # gatk.run()
        else:
            self.ref_name = self.option("ref_genome")
            gatk.set_options({
                "ref_genome": self.ref_name,
                "input_bam": f_path
            })
            # gatk.on("end", self.finish_update, 'gatk')
            # gatk.on("end", self.set_output)
            # self.logger.info("gatk is running!")
            # gatk.run()
        if len(self.gatks) == 1 and self.samples == {}:
            self.gatks[0].on("end", self.finish_update, "gatk")
            self.gatks[0].on("end", self.set_output)
            self.logger.info("gatk is running single!")
        
        if len(self.gatks) == len(self.samples):
            self.on_rely(self.gatks, self.finish_update, 'gatk')
            self.on_rely(self.gatks, self.set_output, "s")
        gatk.run()

    # add by qindanhua  20170103
    def snp_anno(self, event):
        obj = event["bind_object"]
        gatk_output = os.listdir(obj.output_dir)
        vcf_path = ""
        for i in gatk_output:
            if i.endswith(".vcf"):
                vcf_path = os.path.join(obj.output_dir, i)
        self.logger.info(vcf_path)
        annovar = self.add_tool('ref_rna.gene_structure.annovar')
        options = {"ref_genome": self.ref_name, "input_file": vcf_path}
        if self.option("ref_genome") == "customer_mode":
            options["ref_fa"] = self.ref_name
        annovar.set_options(options)
        self.annovars.append(annovar)

    """
    def rename(self, event):
        obj = event["bind_object"]
        self.logger.info("obj is "+ str(obj))
        for f in os.listdir(obj.output_dir):
            old_name = os.path.join(obj.output_dir, f)
            self.logger.info("lalala")  
        self.end() 
    """

    def get_list(self):
        list_path = os.path.join(self.option("fastq_dir").prop["path"], "list.txt")
        file_sample = FileSampleFile()
        file_sample.set_path(list_path)
        samples = file_sample.get_list()
        return samples
    
    def set_output(self, event):
        self.logger.info("set output started!!!")
        obj = event["bind_object"]
        if event['data'] == 's':  # 多个样本
            self.logger.info("现在是多样本分支")
            for tool in self.gatks:
                self.linkdir(tool.output_dir, event['data']+str(self.count), self.output_dir)
                self.count += 1
                self.logger.info("多样本移动文件夹完成！！！")
        else:
            self.logger.info("现在是单样本分支")
            
            for f in os.listdir(obj.output_dir):
                f_path = os.path.join(obj.output_dir, f)
                self.logger.info(f_path)
                self.logger.info(f)
                new_path = os.path.join(self.output_dir, f)
                self.logger.info(new_path)
                if os.path.exists(new_path):
                    self.logger.info("现在进入文件存在分支，module的输出目录下包含文件！！！")
                    os.remove(new_path)
                    self.logger.info("删除文件成功！！！")
                self.logger.info("单个样本，开始移动文件！！！现在不经过判断文件是否存在分支！！！")
                # os.link(f_path, new_path)#单个样本：将gatk的输出结果链接到module的输出目录
                shutil.copy(f_path, self.output_dir)
                self.logger.info("单个样本，移动文件完成！！！")
        self.end()
           
        self.logger.info("set output finished!!!")
        
    def linkdir(self, dirpath, dirname, output_dir):
        files = os.listdir(dirpath)
        newdir = os.path.join(output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in files]
        newfiles = [os.path.join(newdir, i) for i in files]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(files)):
            os.link(oldfiles[i], newfiles[i])
        
    def run(self):
        if self.option("fastq_dir").is_set:
            self.star_multi_run()
            self.logger.info("star multi started!")
       
        else:
            self.star_single_run()
            self.logger.info("star single started!")
        super(SnpRnaModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            [r"./filtered_vcf/", "文件夹", "过滤后的vcf格式的SNP位点文件结果输出目录"]
        ])
        super(SnpRnaModule, self).end()
