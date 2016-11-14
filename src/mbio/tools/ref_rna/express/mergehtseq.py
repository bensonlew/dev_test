# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class MergehtseqAgent(Agent):

    def __init__(self, parent):
        super(MergehtseqAgent, self).__init__(parent)
        options = [
            {"name": "htseq_files", "type":"infile","format": "ref_rna.assembly.bam_dir"},  # 包含所有样本htseq结果文件路径的txt文档
            {"name": "gene_count", "type": "outfile","format": "denovo_rna.express.express_matrix"},  # 输出基因count表
            {"name": "gene_fpkm", "type": "outfile","format": "denovo_rna.express.express_matrix"},  # 输出基因fpkm表
            {"name": "cpu", "type": "int", "default": 10},  # 设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  # 设置内存
        ]
        self.add_option(options)
        self.step.add_steps("mergehtseq")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.mergehtseq.start()
        self.step.update()

    def stepfinish(self):
        self.step.mergehtseq.finish()
        self.step.update()

    def check_options(self):
        if not self.option("htseq_files"):
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
            ["gene_count", "txt",  "输出count表格"],
            ["gene_fpkm", "txt",  "输出fpkm表格"]
        ])
        super(MergehtseqAgent, self).end()

class MergehtseqTool(Tool):

    def __init__(self, config):
        super(MergehtseqTool, self).__init__(config)
        self._version = '1.0.1'
        self.Perl_path ='program/perl/perls/perl-5.24.0/bin/perl '
        self.mergehtseq_path = self.config.SOFTWARE_DIR+'/bioinfo/rna/scripts/merge_htseq1.pl'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH = self.mergehtseq_path)
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)

    def mergehtseq_run(self):
        outdir=os.path.join(self.work_dir,os.path.basename(self.option("htseq_files").prop['path']))
        #os.mkdir(outdir)
        cmd=self.Perl_path+self.mergehtseq_path + " --sample_path %s --output_path %s" % (self.option("htseq_files").prop['path'],
               self.work_dir)
        self.logger.info("开始运行mergehtseq")
        mergehtseq_cmd = self.add_command("mergehtseq", cmd).run()
        self.wait(mergehtseq_cmd)
        if mergehtseq_cmd.return_code == 0:
            self.logger.info("%s运行完成" % mergehtseq_cmd)
        else:
            self.set_error("%s运行出错" % cmd)

    def set_output(self):
        self.logger.info("设置结果目录")
        files = os.listdir(self.work_dir)
        try:
            for root ,dirs,files in os.walk(self.work_dir):
                for file in files:
                    if file.find("count") != -1:
                        file_name=os.path.join(self.work_dir,file)
                        shutil.copy2(file_name,os.path.join(self.output_dir,file))
                        self.option("gene_count").set_path(file_name)
            self.logger.info("设置mergehtseq分析结果目录成功:{}".format(e))
        except Exception as e:
            self.logger.info("设置mergehtseq分析结果目录失败:{}".format(e))


    def run(self):
        super(MergehtseqTool, self).run()
        self.mergehtseq_run()
        self.set_output()
        self.end()
