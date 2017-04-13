#!/usr/bin/python
# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re,subprocess

class TransgenesAgent(Agent):
    def __init__(self, parent):
        super(TransgenesAgent, self).__init__(parent)
        options =[
            {"name":"old_gtf", "type":"infile","format":"ref_rna.reads_mapping.gtf"},
            {"name":"ref_gtf", "type":"infile","format":"ref_rna.reads_mapping.gtf"},
            {"name":"method", "type":"string"},
            {"name":"new_gtf","type":"outfile","format":"ref_rna.reads_mapping.gtf"}
        ]
        self.add_option(options)
        self.step.add_steps("transgenes")
        self.on("start",self.stepstart)
        self.on("end", self.stepfinish)
        
    def stepstart(self):
        self.step.transgenes.start()
        self.step.update()

    def stepfinish(self):
        self.step.transgenes.finish()
        self.step.update()
        
    def check_options(self):
        pass
    
    def set_resource(self):
        self._cpu = 4
        self._memory = '20G'
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            ["new.gtf", "gtf", "新生成merged.gtf"]
        ])
        super(TransgenesAgent, self).end()
        
class TransgenesTool(Tool):
    def __init__(self, config):
        super(TransgenesTool, self).__init__(config)
        self.trans_genes = os.path.join(Config().SOFTWARE_DIR, 'bioinfo/rna/scripts/cufflinks_gene_trans.py ')
        self.python_path = 'program/Python/bin/python '
        
    def trans_run(self):
        self.old_files_path = self.option("old_gtf").prop['path']
        self.new_files_path = self.output_dir+"/"+os.path.basename(self.old_files_path)+"_new"
        cmd = self.python_path+self.trans_genes+" -file_path %s -new_file_path %s -ref_gtf %s -method %s" % \
              (self.old_files_path,self.new_files_path,self.option("ref_gtf").prop['path'], self.option("method"))
        trans_cmd = self.add_command("trans",cmd).run()
        self.wait()
        if trans_cmd.return_code == 0:
            self.logger.info("trans运行成功！")
        else:
            self.logger.info("trans运行失败！")
    
    def set_output(self):
        self.logger.info("设置结果目录")
        if os.path.exists(self.new_files_path):
            self.option("new_gtf", self.new_files_path)
            self.logger.info("设置new_merged.gtf文件成功！")
    
    def run(self):
        super(TransgenesTool, self).run()
        self.trans_run()
        self.set_output()
        self.end()
