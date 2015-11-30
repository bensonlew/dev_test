#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class EstimatorsAgent(Agent):
    """
    estimators:用于生成所有样本的指数表 
    version 1.0  
    author: qindanhua  
    last_modify: 2015.11.10  
    """
    ESTIMATORS = ['sobs', 'chao', 'ace', 'jack', 'bootstrap', 'simpsoneven', 'shannoneven', 'heip', 'smithwilson',
                  'bergerparker', 'shannon', 'npshannon', 'simpson', 'invsimpson', 'coverage', 'qstat']

    def __init__(self, parent):
        super(EstimatorsAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},  # 输入文件
            {"name": "indices", "type": "string", "default": "ace-chao-shannon-simpson"},  # 指数类型
            # {"name": "estimators", "type": "outfile", "format": "meta.alpha_diversity.estimators"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("otutable").is_set:
            raise OptionError("请选择otu表")
        for estimators in self.option('indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class EstimatorsTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(EstimatorsTool, self).__init__(config)
        self.cmd_path = 'meta/alpha_diversity/'
        self.shared_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'
        self.estimator_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'

    def shared(self):
        """
        执行生成shared文件的perl脚本命令
        """
        cmd = os.path.join(self.shared_path, 'otu2shared.pl')
        cmd += ' -i %s -l 0.97 -o otu.shared' % self.option("otutable").prop['path']
        print cmd
        os.system(cmd)

    def mothur(self):
        """
        返回mothur运行生成各样本指数值文件命令
        """
        cmd = '/meta/mothur.1.30 "#summary.single(shared=otu.shared,groupmode=f,calc=%s)"' % (self.option('indices'))
        print cmd
        self.logger.info("开始运行mothur")
        command = self.add_command("mothur", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行mothur完成")
        else:
            self.set_error("运行mothur运行出错!")
        os.system("python %sestimatorsV3.py" % self.estimator_path)
        self.set_output()

    def set_output(self):
        """
        将结果文件链接至output
        """
        self.logger.info("set out put")
        if len(os.listdir(self.output_dir)) != 0:
            os.remove(self.output_dir+'/estimators')
            os.link(self.work_dir+'/estimators', self.output_dir+'/estimators')
        else:
            os.link(self.work_dir+'/estimators', self.output_dir+'/estimators')
            # self.option('estimators').set_path(self.output_dir+'/estimators')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(EstimatorsTool, self).run()
        self.shared()
        self.mothur()
        self.end()
