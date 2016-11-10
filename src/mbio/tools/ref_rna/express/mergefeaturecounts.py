# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class MergefeaturecountsAgent(Agent):

    def __init__(self, parent):
        super(MergefeaturecountsAgent, self).__init__(parent)
        options = [
            {"name": "mergefeaturecounts_files", "type":"infile", "format": "ref_rna.assembly.bam_dir"},  # 包含所有样本featurecounts结果文件路径的txt文档
            {"name": "gene_fpkm", "type": "outfile", "format": "denovo_rna.express.express_matrix"},  # 输出基因fpkm表
            {"name": "gene_count", "type": "outfile", "format": "denovo_rna.express.express_matrix"}  # 输出基因count表
        ]
        self.add_option(options)
        self.step.add_steps("mergefeaturecounts")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.mergefeaturecounts.start()
        self.step.update()

    def stepfinish(self):
        self.step.mergefeaturecounts.finish()
        self.step.update()
    
    def check_options(self):
        if not self.option("mergefeaturecounts_files"):
            raise IOError("Input path is error\n")
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
            ["gene_fpkm", "txt",  "输出FPKM表格"],
            ["gene_count","txt",  "输出count表格"]
        ])
        super(MergefeaturecountsAgent, self).end()

class MergefeaturecountsTool(Tool):
  
    def __init__(self, config):
        super(MergefeaturecountsTool, self).__init__(config)
        self._version = '1.0.1'
        self.Python_path ='program/Python/bin/python '  
        self.mergefeaturecounts_path = self.config.SOFTWARE_DIR+'/bioinfo/rna/scripts/ensembl_count.py'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH = self.mergefeaturecounts_path)
        self.set_environ(PATH = self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        
    def mergefeaturecounts_run(self):
        cmd = self.Python_path + self.mergefeaturecounts_path + " -s {} -m {} -p {}".format(self.option("mergefeaturecounts_files").prop['path'],
                      "featurecounts", self.work_dir)
        self.logger.info("开始运行mergefeaturecounts")
        mergefeaturecounts_cmd = self.add_command("mergefeaturecounts", cmd).run()
        self.wait(mergefeaturecounts_cmd)
        if mergefeaturecounts_cmd.return_code == 0:
            self.logger.info("%s运行完成" % cmd)
        else:
            self.set_error("%s运行出错" % cmd)
    
    def set_output(self):
        self.logger.info("设置结果目录")
        try:
            files =os.listdir(self.work_dir)
            for file in files:
                file_path=os.path.join(self.work_dir, file)
                new_path=os.path.join(self.output_dir, file)
                if file.find("_count") != -1:
                    shutil.copy2(file_path, new_path)
                    self.option("gene_fpkm").set_path(file_path)
                if file.find("_fpkm") != -1:
                    shutil.copy2(file_path,new_path)
                    self.option("gene_count").set_path(file_path)
            self.logger.info("设置mergefeaturecounts分析结果目录成功")
        except Exception as e:
            self.logger.info("设置mergefeaturecounts分析结果目录失败")
            
    def run(self):
        super(MergefeaturecountsTool, self).run()
        self.mergefeaturecounts_run()
        self.set_output()
        self.end()
        
    
                    		
