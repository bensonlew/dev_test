# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class TablemakerballgownAgent(Agent):

    def __init__(self, parent):
        super(TablemakerballgownAgent, self).__init__(parent)
        options = [
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "ref_rna.bam"},  # 样本比对后的bam文件,默认bam格式 "sam"
            {"name": "strand_specific", "type": "bool", "default": False},  # PE测序，是否链特异性, 默认是无特异性 False
            {"name": "strand_dir", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数  "fr-firststrand"  "fr-secondstrand"
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"},  # 设置内存
            {"name": "e_data_ctab", "type": "outfile", "format": "ref_rna.ctab"},
            {"name": "e2t_ctab", "type": "outfile", "format": "ref_rna.ctab"},
            {"name": "t_data_ctab", "type": "outfile", "format": "ref_rna.ctab"},
            {"name": "i2t_ctab", "type": "outfile", "format": "ref_rna.ctab"},
            {"name": "i_data_ctab", "type": "outfile", "format": "ref_rna.ctab"}
        ]
        self.add_option(options)
        self.step.add_steps("tablemakerballgown")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.tablemakerballgown.start()
        self.step.update()

    def stepfinish(self):
        self.step.tablemakerballgown.finish()
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
            [r"ctab$", "", "ctab表"]
        ])
        super(TablemakerballgownAgent, self).end()

class TablemakerballgownTool(Tool):
  
    def __init__(self, config):
        super(TablemakerballgownTool, self).__init__(config)
        self._version = '1.0.1'
        self.tablemaker_path = 'bioinfo/rna/tablemaker-2.1.1/tablemaker'
        self.set_environ(PATH = self.tablemaker_path)
        
    def tablemakerballgown_run(self):
        sample_name_bam_id=os.path.basename(self.option("bam").prop['path']).rfind(".bam")
        sample_name_sam_id=os.path.basename(self.option("bam").prop['path']).rfind(".sam")
        if sample_name_bam_id == -1:
            if sample_name_sam_id == -1:
                raise ValueError("输入文件格式不对")
            else:
                sample_name=os.path.basename(self.option("bam").prop['path'])[:sample_name_sam_id]
        else:
            sample_name=os.path.basename(self.option("bam").prop['path'])[:sample_name_bam_id] 
        output_dir = os.path.join(self.work_dir, sample_name)
        if self.option("strand_specific") == False:
            cmd=self.tablemaker_path + " -p %s -W -G %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                         output_dir, self.option("bam").prop['path'])
        else:
            cmd = self.tablemaker_path + " -p %s -W -G %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                        self.option("strand_dir"), output_dir, self.option("bam").prop['path'])
        self.logger.info("开始运行tablemakerballgown")
        tablemakerballgown_cmd = self.add_command("tablemakerballgown", cmd).run()
        self.wait()
        if tablemakerballgown_cmd.return_code == 0:
            self.logger.info("%s运行完成" % tablemakerballgown_cmd)
        else:
            self.set_error("%s运行出错" % cmd)
            
    
    def set_output(self):
        self.logger.info("设置结果目录")
        sample_name_bam_id=os.path.basename(self.option("bam").prop['path']).rfind(".bam")
        sample_name_sam_id=os.path.basename(self.option("bam").prop['path']).rfind(".sam")
        if sample_name_bam_id == -1:
            if sample_name_sam_id == -1:
                raise ValueError("输入文件格式不对")
            else:
                sample_name=os.path.basename(self.option("bam").prop['path'])[:sample_name_sam_id]
        else:
            sample_name=os.path.basename(self.option("bam").prop['path'])[:sample_name_bam_id] 
        try:
            new_dir=os.path.join(self.output_dir,sample_name)
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)
                self.logger.info(new_dir)
            """e_data_ctab  e2t_ctab  t_data_ctab  i2t_ctab  i_data_ctab """
            pmatch1=re.compile(r'^t_data')
            pmatch2=re.compile(r'^e2t')
            pmatch3=re.compile(r'^i_data')
            pmatch4=re.compile(r'^i2t')
            pmatch5=re.compile(r'^e_data')
            for root, dirs, files in os.walk(self.work_dir):
                for dir in dirs:
                    if dir != "output":
                        files=os.listdir(os.path.join(self.work_dir,dir))
                        for file in files:
                            file_path_dir = os.path.join(self.work_dir, dir)
                            file_path=os.path.join(file_path_dir,file)
                            file_new_path = os.path.join(new_dir, file)
                            if re.match(pmatch1,file):
                                shutil.copy2(file_path, file_new_path)
                                self.option("t_data_ctab").set_path(file_path)
                            if re.match(pmatch2,file):
                                shutil.copy2(file_path, file_new_path)
                                self.option("e2t_ctab").set_path(file_path)
                            if re.match(pmatch3,file):
                                shutil.copy2(file_path, file_new_path)
                                self.option("i_data_ctab").set_path(file_path)
                            if re.match(pmatch4,file):
                                shutil.copy2(file_path, file_new_path)
                                self.option("i2t_ctab").set_path(file_path)
                            if re.match(pmatch5,file):
                                shutil.copy2(file_path, file_new_path)
                                self.option("e_data_ctab").set_path(file_path)               
            self.logger.info("设置tablemakerballgown分析结果目录成功")
        except Exception as e:
            self.logger.info("设置tablemakerballgown分析结果目录失败".format(e))


    def run(self):
        super(TablemakerballgownTool, self).run()
        self.tablemakerballgown_run()
        self.set_output()
        self.end()
        
    
                    		
