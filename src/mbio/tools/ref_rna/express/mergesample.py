# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class MergesampleAgent(Agent):

    def __init__(self, parent):
        super(MergesampleAgent, self).__init__(parent)
        options = [
            {"name": "featurecounts_files", "type":"infile","format": "ref_rna.txt"},  # 输入多个样本featurecounts结果文件的txt文件
            {"name": "gene_count", "type": "outfile","format": "ref_rna.txt"},  # 输出基因count表
            {"name": "gene_fpkm", "type": "outfile","format": "ref_rna.txt"},  # 输出基因fpkm表
            {"name": "exp_way", "type": "string","default": "both"},  # 默认同时输出基因的count和fpkm表 count, fpkm, both
            {"name": "gene_id", "type": "string","default": "ensembl"},  # 默认输出基因的ensembl id
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  # 设置内存
        ]
        self.add_option(options)
        self.step.add_steps("mergesample")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.mergesample.start()
        self.step.update()

    def stepfinish(self):
        self.step.mergesample.finish()
        self.step.update()
    
    def check_options(self):
        if not self.option('featurecounts_files'):
            raise OptionError('必须设置输入文件：所有样本featurecounts结果文件')
        if self.option("exp_way") not in ["both", "fpkm", "count"]:
            raise OptionError("所设置表达量的代表指标不在范围内，请检查")
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
            [r"results$", "xls", "基因的count值"]
        ])
        super(MergesampleAgent, self).end()

class MergesampleTool(Tool):
  
    def __init__(self, config):
        super(MergesampleTool, self).__init__(config)
        self._version = '1.0.1' 
        self.Python_path =self.config.SOFTWARE_DIR + 'program/Python/bin/python '  
        self.mergesample_path = self.config.SOFTWARE_DIR + 'bioinfo/rna/scripts/ensembl_count.py'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH = self.mergesample_path)
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        
    def mergesample(self):
        files = self.option('featurecounts_files').prop['path']
        file_list=[]
        with open(files,"r+") as file1:
            for line in file1:
                line.strip()
                file_list.append(line)
            if len(file_list) == 0:
                raise ValueError("输入文件为空")
        cmd = self.Python_path + self.mergesample_path + " -s %s -m %s -t %s" % (self.option('featurecounts_files').prop['path'], 
                      self.option("exp_way"), self.option("gene_id"))
        output_dir = os.path.join(self.work_dir, "featurecounts_sample_exp")
        mergesamplecounts_cmd = self.add_command("mergesample", cmd).run()
        self.logger.info("开始运行cmd")
        self.logger.info(cmd)
        self.wait()
        if mergesamplecounts_cmd.return_code == 0:
            self.logger.info("%s运行完成" % mergesamplecounts_cmd)
        else:
            self.set_error("%s运行出错" % cmd)
            
    
    def set_output(self):
        self.logger.info("设置结果目录")
        results = os.listdir(self.work_dir)
        try:
            for f in results:
                os.link(self.work_dir + '/' + f, self.output_dir + '/' + f)
            self.logger.info("设置mergesamplecounts分析结果目录成功")
        except Exception as e:
            self.logger.info("设置mergesamplecounts分析结果目录失败".format(e))

    def run(self):
        super(MergesampleTool, self).run()
        self.mergesample()
        self.set_output()
        self.end()
        
    
                    		
