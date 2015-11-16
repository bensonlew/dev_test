# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class RdaCcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.5
    """

    def __init__(self, parent):
        super(RdaCcaAgent, self).__init__(parent)
        options = [
            {"name": "input1", "type": "infile", "format": "otu_table"},
            # 输入文件otu表
            {"name": "input2", "type": "infile", "format": "env_table"},
            # 输入文件，可选，环境因子表
            {"name": "output", "type": "outfile", "format": "rdacca_outdir"}
            # 包括样本，物种和环境因子坐标和RDA 或者CCA轴权重值
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError('必须提供otu表')
        if not self.option('input2').is_set:
            raise OptionError('必须提供环境因子表')
        if not self.option('output').is_set:
            raise OptionError('必须指定输出文件夹')  # 没有检查是不是文件夹

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 需要资源数暂时不清楚
        self._memory = ''  #


class RdaCcaTool(Tool):

    def __init__(self, config):
        super(RdaCcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = 'meta/ordination.pl'  # 暂不确定

    def run(self):
        """
        运行
        :return:
        """
        super(RdaCcaTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        :return:
        """
        cmd = self.cmd_path
        cmd += ' -type rdacca -community %s -environment %s -outdir %s' % (
            self.option('input1'), self.option('input2'),
            self.option('output'))
        ordination_command = self.add_command('ordination_rda_cca', cmd)
        ordination_command.run()
        self.wait()
        if ordination_command.return_code == 0:
            self.logger.info('运行ordination.pl程序计算rdacca完成')
            self.end()
        else:
            self.set_error('运行ordination.pl程序计算rdacca出错')
