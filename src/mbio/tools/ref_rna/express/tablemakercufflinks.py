# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re

class TablemakercufflinksAgent(Agent):

    def __init__(self, parent):
        super(TablemakercufflinksAgent, self).__init__(parent)
        options = [
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "ref_rna.bam"},  # 样本比对后的bam文件,默认bam格式 "sam"
            {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是无特异性 False
            {"name": "strand_dir", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数  "fr-firststrand"  "fr-secondstrand"
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"},  # 设置内存
            {"name": "gene_fpkm","type": "outfile","format": "ref_rna.txt"}
        ]
        self.add_option(options)
        self.step.add_steps("tablemakercufflinks")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.tablemakercufflinks.start()
        self.step.update()

    def stepfinish(self):
        self.step.tablemakercufflinks.finish()
        self.step.update()
    
    def check_options(self):
        if not self.option("ref_gtf").is_set:
            raise OptionError("需要输入gtf文件")    
        return True
        
    def set_resource(self):
        self._cpu = 10
        self._memory = '100G'
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"gene", "", "基因的fpkm表"]
        ])
        super(TablemakercufflinksAgent, self).end()

class TablemakercufflinksTool(Tool):
  
    def __init__(self, config):
        super(TablemakercufflinksTool, self).__init__(config)
        self._version = '1.0.1'
        self.tablemaker_path = 'bioinfo/rna/tablemaker-2.1.1/tablemaker'
        self.set_environ(PATH = self.tablemaker_path)
        
    def tablemakercufflinks_run(self):
        sample_name_bam_id = os.path.basename(self.option("bam").prop['path']).rfind(".bam")
        sample_name_sam_id = os.path.basename(self.option("bam").prop['path']).rfind(".sam")
        if sample_name_bam_id == -1:
            if sample_name_sam_id == -1:
                raise ValueError("输入文件格式不对")
            else:
                sample_name = os.path.basename(self.option("bam").prop['path'])[:sample_name_sam_id]
        else:
            sample_name=os.path.basename(self.option("bam").prop['path'])[:sample_name_bam_id] 
        output_dir = os.path.join(self.work_dir, sample_name)
        if self.option("strand_specific") == False:
            cmd = self.tablemaker_path + " -p %s -g %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                       output_dir, self.option("bam").prop['path'])
        else:
            cmd = self.tablemaker_path + " -p %s -g %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                            self.option("strand_dir"), output_dir, self.option("bam").prop['path'])
        self.logger.info("开始运行tablemakercufflinks")
        tablemakercufflinks_cmd = self.add_command("tablemakercufflinks", cmd).run()
        self.wait()
        if tablemakercufflinks_cmd.return_code == 0:
            self.logger.info("%s运行完成" % tablemakercufflinks_cmd)
        else:
            self.set_error("%s运行出错" % cmd)
            
    
    def set_output(self):
        self.logger.info("设置结果目录")
        sample_name_bam_id = os.path.basename(self.option("bam").prop['path']).rfind(".bam")
        sample_name = os.path.basename(self.option("bam").prop['path'])[:sample_name_bam_id]
        try:
            for root, dirs, files in os.walk(self.work_dir):
                if dirs.find("sample") != -1:
                    self.logger.info(dirs)
                    for file in files:
                        if file.find("genes") != -1:
                            #file_dir = os.path.join(self.output_dir, sample_name)                                
                            new_name = os.rename(file, sample_name + "_" + file)
                            file_path = os.path.join(self.work_dir, dirs)
                            shutil.copy2(os.path.join(file_path, new_name), os.path.join(self.output_dir, new_name))                                
                            self.option("gene_fpkm").set_path(os.path.join(file_path, new_name))
            self.logger.info("设置tablemakercufflinks分析结果目录成功")
        except Exception as e:
            self.logger.info("设置tablemakercufflinks分析结果目录失败".format(e))


    def run(self):
        super(TablemakercufflinksTool, self).run()
        self.tablemakercufflinks_run()
        self.set_output()
        self.end()
        
    
                    		
