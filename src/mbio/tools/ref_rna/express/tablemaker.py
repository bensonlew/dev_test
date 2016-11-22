# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re

class TablemakerAgent(Agent):

    def __init__(self, parent):
        super(TablemakerAgent, self).__init__(parent)
        options = [
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "ref_rna.bam"},  # 样本比对后的bam文件,默认bam格式 "sam"
            {"name": "strand_specific", "type": "string", "default": "fr-unstranded"},  # PE测序，是否链特异性, 默认是无特异性 “None”
            {"name": "firststrand", "type": "string", "default": "None"},  # 链特异性时选择正链, 默认不设置此参数  "fr-firststrand"
            {"name": "secondstrand", "type": "string", "default": "None"},  # 链特异性时选择负链, 默认不设置此参数  "fr-secondstrand"
            {"name": "express_ballgown", "type": "bool", "default": True},  # 是否生成ctab文件，传递给ballgown计算转录本差异表达分析差异表达分析
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"},  # 设置内存
            {"name": "e_data_ctab","type":"outfile","format":"ref_rna.ctab"},
            {"name":"e2t_ctab","type":"outfile","format":"ref_rna.ctab"},
            {"name":"t_data_ctab","type":"outfile","format":"ref_rna.ctab"},
            {"name":"i2t_ctab","type":"outfile","format":"ref_rna.ctab"},
            {"name":"i_data_ctab","type":"outfile","format":"ref_rna.ctab"},
            {"name":"gene_fpkm","type":"outfile","format":"ref_rna.txt"}
        ]
        self.add_option(options)
        self.step.add_steps("tablemaker")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.tablemaker.start()
        self.step.update()

    def stepfinish(self):
        self.step.tablemaker.finish()
        self.step.update()
    
    def check_options(self):
        if not self.option("ref_gtf").is_set:
            raise OptionError("需要输入gtf文件")
        if self.option("express_ballgown") not in [True, False]:
            raise OptionError("输入为逻辑值")
        if self.option("strand_specific") == "None":
            if self.option("firststrand") == "None" and self.option("secondstrand") == "None":
                raise OptionError("请设置正链或负链")     
        if self.option("firststrand") == "fr-firststrand" and self.option("secondstrand") == "fr-secondstrand":
            raise OptionError("链特异性不能同时选择正链和负链")
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
        super(TablemakerAgent, self).end()

class TablemakerTool(Tool):
  
    def __init__(self, config):
        super(TablemakerTool, self).__init__(config)
        self._version = '1.0.1'
        self.tablemaker_path = 'bioinfo/rna/tablemaker-2.1.1/tablemaker'
        self.set_environ(PATH = self.tablemaker_path)
        
    def tablemaker_run(self):
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
        if self.option("express_ballgown") == True:
            if self.option("strand_specific") == "fr-unstranded":
                cmd=self.tablemaker_path + " -p %s -W -G %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                         output_dir, self.option("bam").prop['path'])
            else:
                if self.option("strand_specific") == "None":
                    if self.option("firststrand") == "fr-firststrand":
                        cmd = self.tablemaker_path + " -p %s -W -G %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                               self.option("firststrand"), output_dir, self.option("bam").prop['path'])
                    else:
                        cmd = self.tablemaker_path + " -p %s -W -G %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                               self.option("secondstrand"), output_dir, self.option("bam").prop['path'])
        else:
            if self.option("strand_specific") == "fr-unstranded":
                cmd=self.tablemaker_path + " -p %s -g %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                       output_dir, self.option("bam").prop['path'])
            else:
                if self.option("strand_specific") == "None":
                    if self.option("firststrand") == "fr-firststrand":
                        cmd = self.tablemaker_path + " -p %s -g %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                            self.option("firststrand"), output_dir, self.option("bam").prop['path'])
                    else:
                        cmd = self.tablemaker_path + " -p %s -g %s --library-type %s -o %s %s" % (self.option("cpu"), self.option("ref_gtf").prop['path'],
                         self.option("secondstrand"), output_dir, self.option("bam").prop['path'])
        self.logger.info("开始运行tablemaker")
        tablemaker_cmd = self.add_command("tablemaker", cmd).run()
        self.wait()
        if tablemaker_cmd.return_code == 0:
            self.logger.info("%s运行完成" % tablemaker_cmd)
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
            if self.option("express_ballgown") == True:
                new_dir=os.path.join(self.output_dir,sample_name)
                if not os.path.exists(new_dir):
                    os.mkdir(new_dir)
                """e_data_ctab  e2t_ctab  t_data_ctab  i2t_ctab  i_data_ctab """
                for root,dirs,files in os.walk(self.work_dir):
                    pmatch1=re.compile(r'^t_data')
                    pmatch2=re.compile(r'^e2t')
                    pmatch3=re.compile(r'^i_data')
                    pmatch4=re.compile(r'^i2t')
                    pmatch5=re.compile(r'^e_data')
                    for file in files:
                        if file.find("ctab") != -1:
                            file_path=os.path.join(self.work_dir,file)
                            out_path=os.path.join(new_dir,file)
                            shutil.copy2(file_path,out_path)
                            if re.match(pmatch1,file):
                                self.option("t_data_ctab").set_path(file_path)
                            if re.match(pmatch2,file):
                                self.option("e2t_ctab").set_path(file_path)
                            if re.match(pmatch3,file):
                                self.option("i_data_ctab").set_path(file_path)
                            if re.match(pmatch4,file):
                                self.option("i2t_ctab").set_path(file_path)
                            if re.match(pmatch5,file):
                                self.option("e_data_ctab").set_path(file_path)
            else:
                for root,dirs,files in os.walk(self.work_dir):
                    if dirs.find("sample") != -1:
                        for file in files:
                            if file.find("genes") != -1:
                                file_dir=os.path.join(self.output_dir,sample_name)                                
                                new_name=os.rename(file,sample_name+"_"+file)
                                file_path=os.path.join(self.work_dir,dirs)
                                shutil.copy2(os.path.join(file_path,new_name),os.path.join(self.output_dir,new_name))                                
                                self.option("gene_fpkm").set_path(os.path.join(file_path,new_name))
            self.logger.info("设置tablemaker分析结果目录成功")
        except Exception as e:
            self.logger.info("设置tablemaker分析结果目录失败".format(e))


    def run(self):
        super(TablemakerTool, self).run()
        self.tablemaker_run()
        self.set_output()
        self.end()
        
    
                    		
