#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import subprocess
import re


class EstimatorsAgent(Agent):
    """
    estimators:用于生成所有样本的指数表
    version 1.0
    author: qindanhua
    last_modify: 2015.12.10 by yuguo
    """
    ESTIMATORS = ['sobs', 'chao', 'ace', 'jack', 'bootstrap', 'simpsoneven', 'shannoneven', 'heip', 'smithwilson',
                  'bergerparker', 'shannon', 'npshannon', 'simpson', 'invsimpson', 'coverage', 'qstat']

    def __init__(self, parent):
        super(EstimatorsAgent, self).__init__(parent)
        options = [
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},  # 输入文件
            {"name": "indices", "type": "string", "default": "ace-chao-shannon-simpson-coverage"},  # 指数类型
            {"name": "level", "type": "string", "default": "otu"}  # level水平
            # {"name": "estimators", "type": "outfile", "format": "meta.alpha_diversity.estimators"}  # 输出结果
        ]
        self.add_option(options)
        self.step.add_steps('estimators')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.estimators.start()
        self.step.update()

    def step_end(self):
        self.step.estimators.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("otu_table").is_set:
            raise OptionError("请选择otu表")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class', 'order',
                                        'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        for estimators in self.option('indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 11
        self._memory = ''


class EstimatorsTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(EstimatorsTool, self).__init__(config)
        self.cmd_path = 'meta/alpha_diversity/'
        self.scripts_path = 'meta/scripts/'
        # self.step.add_steps('shared', 'estimators')

    def shared(self):
        """
        执行生成shared文件的perl脚本命令
        """
        otu_table = self.option("otu_table").prop['path']
        if self.option("otu_table").format == "meta.otu.tax_summary_dir":
            otu_table = self.option("otu_table").get_table(self.option("level"))
        self.logger.info("otutable format:{}".format(self.option("otu_table").format))
        self.logger.info("转化otu_table({})为shared文件({})".format(otu_table, "otu.shared"))
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR+"/meta/scripts/otu2shared.pl "+" -i "+otu_table +
                                    " -l 0.97 -o " + self.option("level")+".shared", shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("转化otu_table到shared文件出错")
            return False

    def mothur_check(self, command, line):
        if re.match(r"\[ERROR\]:", line):
            command.kill()
            self.set_error("mothur命令报错")

    def mothur(self):
        """
        运行mothur软件生成各样本指数表
        """
        cmd = '/meta/mothur.1.30 "#summary.single(shared=otu.shared,groupmode=f,calc=%s)"' % (self.option('indices'))
        print cmd
        self.logger.info("开始运行mothur")
        command = self.add_command("mothur", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行mothur完成")
            try:
                subprocess.check_output("python "+self.config.SOFTWARE_DIR+"/meta/scripts/make_estimate_table.py ",
                                        shell=True)
                self.logger.info("OK")
                self.set_output()
                return True
            except subprocess.CalledProcessError:
                self.logger.info("生成estimate文件出错!")
                self.set_error("生成estimate文件出错!")
                return False
        else:
            self.set_error("运行mothur运行出错!")
            return False

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set output")
        if len(os.listdir(self.output_dir)) != 0:
            os.remove(self.output_dir+'/estimators.xls')
            os.link(self.work_dir+'/estimators.xls', self.output_dir+'/estimators.xls')
        else:
            os.link(self.work_dir+'/estimators.xls', self.output_dir+'/estimators.xls')
            # self.option('estimators').set_path(self.output_dir+'/estimators')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(EstimatorsTool, self).run()
        if self.shared():
            if self.mothur():
                self.end()
        else:
            self.set_error("shared运行出错!")
