# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class MergecufflinksAgent(Agent):

    def __init__(self, parent):
        super(MergecufflinksAgent, self).__init__(parent)
        options = [
            {"name": "cufflinks_files", "type":"infile","format": "ref_rna.assembly.bam_dir"},  # 包含所有样本cufflinks结果文件路径的txt文档
            {"name": "gene_fpkm", "type": "outfile","format": "ref_rna.txt"},  # 输出基因fpkm表
        ]
        self.add_option(options)
        self.step.add_steps("mergecufflinks")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.mergecufflinks.start()
        self.step.update()

    def stepfinish(self):
        self.step.mergecufflinks.finish()
        self.step.update()

    def check_options(self):
        if not self.option("cufflinks_files"):
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
            ["gene_fpkm", "txt",  "输出FPKM表格"]
        ])
        super(MergecufflinksAgent, self).end()

class MergecufflinksTool(Tool):

    def __init__(self, config):
        super(MergecufflinksTool, self).__init__(config)
        self._version = '1.0.1'
        self.Python_path ='program/Python/bin/python '
        self.mergecufflinks_path = self.config.SOFTWARE_DIR+'/bioinfo/rna/scripts/ensembl_count.py'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        self.set_environ(PATH = self.mergecufflinks_path)
        self.set_environ(PATH = self.gcc, LD_LIBRARY_PATH=self.gcc_lib)

    def mergecufflinks_run(self):
        cmd = self.Python_path + self.mergecufflinks_path + " -s {} -m {} -p {}".format(self.option("cufflinks_files").prop['path'],
                      "cufflinks", self.work_dir)
        self.logger.info("开始运行mergecufflinks")
        mergecufflinks_cmd = self.add_command("mergecufflinks", cmd).run()
        self.wait(mergecufflinks_cmd)
        if mergecufflinks_cmd.return_code == 0:
            self.logger.info("%s运行完成" % cmd)
        else:
            self.set_error("%s运行出错" % cmd)

    def set_output(self):
        self.logger.info("设置结果目录")
        try:
            for root,dirs,files in os.walk(self.work_dir):
                for f in files:
                    if f.find('fpkm')!=-1:
                        f_path=os.path.join(self.work_dir, f)
                        shutil.copy2(f_path,self.output_dir)
                        self.option('gene_fpkm').set_path(f_path)
            self.logger.info("设置mergetablemaker分析结果目录成功")
        except Exception as e:
            self.logger.info("设置mergetablemaker分析结果目录失败".format(e))

    def run(self):
        super(MergecufflinksTool, self).run()
        self.mergecufflinks_run()
        self.set_output()
        self.end()
