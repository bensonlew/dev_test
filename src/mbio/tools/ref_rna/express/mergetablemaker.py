# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class MergetablemakerAgent(Agent):

    def __init__(self, parent):
        super(MergetablemakerAgent, self).__init__(parent)
        options = [
            {"name": "tablemaker_files", "type":"infile","format": "ref_rna.assembly.bam_dir"},  # 包含所有样本tablemaker结果文件路径的txt文档
            {"name": "gene_fpkm", "type": "outfile", "format": "ref_rna.txt"},  # 生成的基因FPKM矩阵
            {"name": "cpu", "type": "int", "default": 4},  #设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  #设置内存
        ]
        self.add_option(options)
        self.step.add_steps("mergetablemaker")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.mergetablemaker.start()
        self.step.update()

    def stepfinish(self):
        self.step.mergetablemaker.finish()
        self.step.update()
    
    def check_options(self):
        
        return True
        
    def set_resource(self):
        self._cpu = 4
        self._memory = '100G'
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [".", "txt", "基因的count值"]
        ])
        super(MergetablemakerAgent, self).end()

class MergetablemakerTool(Tool):
  
    def __init__(self, config):
        super(MergetablemakerTool, self).__init__(config)
        self._version = '1.0.1'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.tablemaker_path = 'bioinfo/rna/tablemaker-0.6.1/scripts/'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.set_environ(PATH = self.tablemaker_path)
        
    def mergetablemaker_run(self):
        
        self.logger.info("开始运行mergetablemaker计算表达量")
        tablemaker_cmd = self.add_command("mergetablemaker", cmd).run()
        self.wait()
        if tablemaker_cmd.return_code == 0:
            self.logger.info("%s运行完成" % tablemaker_cmd)
        else:
            self.set_error("%s运行出错" % cmd)
            
    
    def set_output(self):
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)
        try:
            for f in results:
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            self.logger.info("设置mergetablemaker分析结果目录成功")
        except Exception as e:
            self.logger.info("设置mergetablemaker分析结果目录失败".format(e))

    def run(self):
        super(MergetablemakerTool, self).run()
        self.mergetablemaker_run()
        self.set_output()
        self.end()
        
    
                    		
