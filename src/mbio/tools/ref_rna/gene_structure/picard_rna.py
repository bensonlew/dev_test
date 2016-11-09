# -*- coding: utf-8 -*-
# __author__ = 'chenyanyan'
# last_modifiy:2016.09.28

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import shutil 
import re 
# import subprocess
# import glob

class PicardRnaAgent(Agent):
    """
    samtools 处理mapping生成的bam文件软件，将传入GATK软件之前的bam文件进行一系列处理，使之符合SNP分析要求
    """


    def __init__(self, parent):
        super(PicardRnaAgent, self).__init__(parent)
        
        self._ref_genome_lst = ["customer_mode", "Chicken", "Tilapia", "Zebrafish", "Cow", "Pig", "Fruitfly", "Human", "Mouse", "Rat", "Arabidopsis", "Broomcorn",\
        "Rice", "Zeamays", "Test"]
        
        options = [
        
            {"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"}, #用户上传参考基因组文件
            {"name":"ref_genome", "type":"string"},#参考基因组模式选项 用户自定义、选择已有生物物种
            {"name":"in_sam","type":"infile","format":"align.bwa.sam"} #输入用于排序的sam文件
            # "name":"head","type":"string","default":""} 头文件信息
        ]
        self.add_option(options)
        self.step.add_steps('picard_rna')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        
    def step_start(self):
        self.step.picard_rna.start()
        self.step.update()

    def step_end(self):
        self.step.picard_rna.finish()
        self.step.update()    
        
    def check_options(self):
       
        if not self.option("in_sam").is_set:
            raise OptionError("请输入用于分析的sam文件！")
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
            raise OptionError("请输入自定义参考基因组序列文件！")
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请选择参考基因组！") 
        
    def set_resource(self):
        self._cpu = 10
        self._memory = '100G'
        
    def end(self):
      
        super(PicardRnaAgent, self).end()  
        
class PicardRnaTool(Tool):
    def __init__(self, config):
        super(PicardRnaTool, self).__init__(config)
        self.picard_path = "/mnt/ilustre/users/sanger-dev/sg-users/chenyanyan/"  
    
    def dict(self, dict_name):
        
        cmd = "program/sun_jdk1.8.0/bin/java -jar {}picard.jar CreateSequenceDictionary R={} O={}".format(self.picard_path, self.option("ref_genome_custom").prop["path"], dict_name)
        print cmd
        self.logger.info("开始用picard对参考基因组构建字典")
        command = self.add_command("dict", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("参考基因组构建dict done!")
        else:
            self.set_error("构建dict过程error！")
        
            
    
    def addorreplacereadgroups(self):
       
        cmd = "program/sun_jdk1.8.0/bin/java -jar {}picard.jar AddOrReplaceReadGroups I={} O={} SO=coordinate LB=HG19 PL=illumina PU=HG19 SM=HG19".format(self.picard_path, self.option("in_sam").prop["path"], "add_sorted.bam")
        print cmd
        self.logger.info("使用picard对sam文件进行加头和排序")
        command = self.add_command("addorreplacereadgroups", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("sam文件addorreplacereadgroups完成!")
        else:
            self.set_error("sam文件addorreplacereadgroups出错！")
    
    def markduplicates(self, add_sorted_bam):
        """
       
        """
        cmd = "program/sun_jdk1.8.0/bin/java -jar {}picard.jar MarkDuplicates I={} O={} CREATE_INDEX=true VALIDATION_STRINGENCY=SILENT M=dedup_add_sorted.metrics".format(self.picard_path, add_sorted_bam, \
        "dedup_add_sorted.bam")
        print cmd
        self.logger.info("使用picard对bam文件进行重复标记")
        command = self.add_command("markduplicates", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("sam文件MarkDuplicates完成!")
        else:
            self.set_error("sam文件MarkDuplicates出错！")
        
    
    """
    def set_output():
        
        设置输出结果路径
        
    """

    def run(self):
        """
        运行
        """
        super(PicardRnaTool, self).run()
        
        
        if self.option("ref_genome") == "customer_mode" and self.option("ref_genome_custom").is_set:
            dict_name = os.path.splitext(os.path.split(self.option("ref_genome_custom").prop["path"])[-1])[0] + ".dict" #是一个字符串
            self.logger.info("参考基因组为自定义模式的情况下建字典！")
            self.dict(dict_name)
            if os.path.exists(os.path.join(self.work_dir, dict_name)):       
                shutil.copy(os.path.join(self.work_dir, dict_name), os.path.split(self.option("ref_genome_custom").prop["path"])[0])  #把生成的字典移动到和输入的fasta文件一个目录下
                self.logger.info("开始移动字典文件")
        
        self.logger.info("运行addorreplacereadgroups")
        if self.option("in_sam").is_set:
            self.addorreplacereadgroups()
            
        self.logger.info("运行MarkDuplicates")
        if os.path.exists(os.path.join(self.work_dir, "add_sorted.bam")):
            bam_path = os.path.join(self.work_dir, "add_sorted.bam") 
            self.markduplicates(bam_path)
            
        outputs = os.listdir(os.getcwd())
        for i in outputs:
            if re.match(r"dedup_add_sorted*", i):
                shutil.copy(i, self.output_dir)
                
        self.end()
    
      
    
        
        
     
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
