# -*- coding: utf-8 -*-
# __author__ = 'chenyanyan'
# last modify 2016.09.28

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import glob
from biocluster.core.exceptions import OptionError
import subprocess
import re 
import shutil
import json

class GatkAgent(Agent):
    """
    SNP工具,gatk的每一步里面的R都是fa文件和它的字典一起的，如若不是，gatk将无法正确运行
    """
    def __init__(self, parent):
        super(GatkAgent, self).__init__(parent)
        self._ref_genome_lst = ["customer_mode", "Chicken", "Tilapia", "Zebrafish", "Cow", "Pig", "Fruitfly", "Human", "Mouse", "Rat", "Arabidopsis", "Broomcorn",\
        "Rice", "Zeamays", "Test"]
        options = [
            #{"name":"ref_genome_custom", "type": "infile", "format": "sequence.fasta"},
            {"name":"ref_genome", "type":"string"},
            {"name": "ref_fa", "type": "infile", "format": "sequence.fasta"},  # 参考序列,需要在picard所建立的dict和fai文件一起
            {"name": "input_bam", "type": "infile", "format": "align.bwa.bam"},  # bam文件类型，输入 
        ]
        self.add_option(options)
        self.step.add_steps('gatk')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        
    def step_start(self):
        self.step.gatk.start()
        self.step.update()

    def step_end(self):
        self.step.gatk.finish()
        self.step.update()
        
    def check_options(self):
        
        if not self.option("ref_genome") in self._ref_genome_lst:
            raise OptionError("请选择参考基因组！")
        if self.option("ref_genome") == "customer_mode" and not self.option("ref_fa").is_set:
            raise OptionError("自定义参考基因组文件未提供！")
        if not self.option("input_bam").is_set:
            raise OptionError("用于分析的bam文件未提供！")
        
            
    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = '200G'
        
    def end(self):
      
        super(GatkAgent, self).end()    
        
class GatkTool(Tool):
    """
    GATK3.6
    """
    def __init__(self, config):
        super(GatkTool, self).__init__(config)
        self.gatk_path = "/mnt/ilustre/users/sanger-dev/sg-users/chenyanyan/GATK3.6/"
        self.java_path = "program/sun_jdk1.8.0/bin/"
        self.samtools_path = "/bioinfo/align/samtools-1.3.1/samtools"
        
    def samtools_faidx(self, ref_fasta):
        cmd = "{} faidx {}".format(self.samtools_path, ref_fasta)
        self.logger.info("开始进行samtools建索引！")
        command = self.add_command("samtools_faidx", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("samtools建索引完成！")
        else:
            self.set_error("samtools建索引出错！")
        
        
    def gatk_split(self, ref_fasta):
        """
        step0:
        """
        cmd = "{}java -jar {}GenomeAnalysisTK.jar -T SplitNCigarReads -R {} -I {} -o {} -rf ReassignOneMappingQuality -RMQF 255 -RMQT 60 -U ALLOW_N_CIGAR_READS".format(self.java_path, self.gatk_path, ref_fasta,\
        self.option("input_bam").prop["path"], "split.bam")
        self.logger.info("开始进行split NCigar reads")
        command = self.add_command("splitncigarreads", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("split NCigar reads完成！")
        else:
            self.set_error("split NCigar reads出错！")
        
    def gatk_vc(self, ref_fasta, split_bam):
        """
        step1：
        """
        cmd = "{}java -jar {}GenomeAnalysisTK.jar -T HaplotypeCaller -R {} -I {} -dontUseSoftClippedBases -stand_call_conf 20.0 -stand_emit_conf 20.0 -o {}".format(self.java_path, self.gatk_path, ref_fasta, split_bam, "output.vcf")
        self.logger.info("开始进行variant calling")
        command = self.add_command("variant calling", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("variant calling完成！")
        else:
            self.set_error("variant calling出错！")
            
    def gatk_vf(self, ref_fasta, variantfile):
        """
        step2：
        """
        cmd = self.config.SOFTWARE_DIR + "/"
        cmd += "{}java -jar {}GenomeAnalysisTK.jar -R {} -T VariantFiltration -V {} -window 35 -cluster 3 -filterName FS -filter 'FS > 30.0' -filterName QD -filter 'QD < 2.0' -o {}".format(self.java_path, self.gatk_path, ref_fasta, variantfile, "filtered.vcf")
        self.logger.info("开始进行variant filtering")
        
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('variant filtering成功')
        except subprocess.CalledProcessError:
            self.logger.info('variant filtering失败')
            self.set_error('无法生成vcf文件')
            
    def run(self):
        
        super(GatkTool, self).run()
        
        if self.option("ref_genome") == "customer_mode":
            ref = self.option("ref_fa").prop["path"]
            self.samtools_faidx(ref)
            ref_fai = os.path.split(ref)[-1] + ".fai" #fai文件的名
            if os.path.exists(os.path.join(self.work_dir, ref_fai)):
                shutil.copy(os.path.join(self.work_dir, ref_fai), os.path.split(ref)[0]) #移动建好的fai文件与参考基因组在同一目录下
            self.gatk_split(ref)
            if os.path.exists(os.path.join(self.work_dir, "split.bam")):
                split_bam = os.path.join(self.work_dir, "split.bam")
            self.gatk_vc(ref, split_bam)
            if os.path.exists(os.path.join(self.work_dir, "output.vcf")):
                vcf_path = os.path.join(self.work_dir, "output.vcf")
            self.gatk_vf(ref, vcf_path)
        
        
                
        else:
            #ref = self.option("ref_genome")
            self.logger.info("在参考基因组从数据库中选择时，运行star")
            with open("/mnt/ilustre/users/sanger-dev/sg-users/chenyanyan/refGenome/ref_genome.json","r") as f:
                dict = json.loads(f.read())
                ref = dict[self.option("ref_genome")]["ref_dict"]  #是ref_dict的路径，作为参数传给比对函数
            self.gatk_split(ref)
            if os.path.exists(os.path.join(self.work_dir, "split.bam")):
                split_bam = os.path.join(self.work_dir, "split.bam")
            self.gatk_vc(ref, split_bam)
            if os.path.exists(os.path.join(self.work_dir, "output.vcf")):
                vcf_path = os.path.join(self.work_dir, "output.vcf")
            self.gatk_vf(ref, vcf_path)
                
        outputs = os.listdir(os.getcwd())
        for i in outputs:
            if re.match(r"filtered.vcf*", i):
                shutil.copy(i, self.output_dir)
     
        self.end()
            
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

   
            

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    