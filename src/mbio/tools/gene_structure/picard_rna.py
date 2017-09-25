# -*- coding: utf-8 -*-
# __author__ = 'chenyanyan'
# last_modifiy:2016.09.28

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import shutil 
import re

class PicardRnaAgent(Agent):
    """
    samtools 处理mapping生成的bam文件软件，将传入GATK软件之前的bam文件进行一系列处理，使之符合SNP分析要求
    """
    def __init__(self, parent):
        super(PicardRnaAgent, self).__init__(parent)
        options = [
            {"name": "in_bam", "type": "infile", "format": "align.bwa.bam"}  # 输入用于排序的bam文件
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
       
        if not self.option("in_bam").is_set:
            raise OptionError("请输入用于分析的sam文件！")
        # if self.option("ref_genome") == "customer_mode" and not self.option("ref_genome_custom").is_set:
        #     raise OptionError("请输入自定义参考基因组序列文件！")
        # if not self.option("ref_genome") in self._ref_genome_lst:
        #     raise OptionError("请选择参考基因组！")
        
    def set_resource(self):
        self._cpu = 10
        self._memory = '50G'
        
    def end(self): 
        super(PicardRnaAgent, self).end()


class PicardRnaTool(Tool):

    def __init__(self, config):
        super(PicardRnaTool, self).__init__(config)
        self.picard_path = self.config.SOFTWARE_DIR + "/bioinfo/gene-structure/"
        self.sample_name = ''

    def addorreplacereadgroups(self):
        self.sample_name = os.path.basename(self.option("in_bam").prop["path"])[:-4]
        self.logger.info(self.sample_name)
        cmd = "program/sun_jdk1.8.0/bin/java -jar {}picard.jar AddOrReplaceReadGroups I={} O={} SO=coordinate LB=HG19 PL=illumina PU=HG19 VALIDATION_STRINGENCY=SILENT SM={}".format(self.picard_path, self.option("in_bam").prop["path"], "add_sorted.bam", self.sample_name)
        self.logger.info("使用picard对sam文件进行加头和排序")
        command = self.add_command("addorreplacereadgroups", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("sam文件addorreplacereadgroups完成!")
        else:
            command.rerun()
            if command.return_code == 0:
                self.logger.info("sam文件addorreplacereadgroups完成!")
            else:
                self.set_error("sam文件addorreplacereadgroups出错！")
                raise Exception("sam文件addorreplacereadgroups出错！")
    
    def markduplicates(self, add_sorted_bam):
        """
       
        """
        # 增加MAX_FILE_HANDLES_FOR_READ_ENDS_MAP参数， 原因是投递节点打开文件数目限制

        os.system("ulimit -n 65536")
        self.logger.info("提高临时文件数量限制")
        #command = self.add_command("ulimit_num", cmd)
        #command.run()
        #self.wait()
        #if command.return_code == 0:
        #    self.logger.info("完成提高文件数量!")
        #else:
        #    self.set_error("提高文件数量 ulimit -n 65536出错！")

        cmd = "program/sun_jdk1.8.0/bin/java -jar {}picard.jar MarkDuplicates I={} O={} CREATE_INDEX=true VALIDATION_STRINGENCY=SILENT MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=500 M=dedup_add_sorted.metrics".format(self.picard_path, add_sorted_bam, \
        "dedup_add_sorted.bam")
        self.logger.info("使用picard对bam文件进行重复标记")
        command = self.add_command("markduplicates", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("bam文件MarkDuplicates完成!")
        else:
            self.set_error("bam文件MarkDuplicates出错！")

    def run(self):
        """
        运行
        """
        super(PicardRnaTool, self).run()
       
        self.logger.info("运行addorreplacereadgroups")
        if self.option("in_bam").is_set:
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
