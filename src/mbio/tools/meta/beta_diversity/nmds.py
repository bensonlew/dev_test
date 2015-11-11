# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class NmdsAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.5
    """

    def __init__(self, parent):
        super(NmdsAgent, self).__init__()
        options = [
            {"name": "input", "type": "infile", "format": "distance_matrix"},
            # 输入文件距离矩阵
            {"name": "output", "type": "outfile", "format": "Nmds_outdir"},
            # 样本的坐标表
            # 目前没有关于计算维度的设置

        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input').is_set:
            raise OptionError(u'必须提供输入距离矩阵表')
        if not self.option('output').is_set:
            raise OptionError(u'必须指定输出文件夹')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 需要资源数暂时不清楚
        self._memory = ''  #


class NmdsTool(Tool):

    def __init__(self, config):
        super(NmdsTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = 'meta/ordination.pl'  # 暂不确定

    def run(self):
        """
        运行
        :return:
        """
        super(NmdsTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        :return:
        """
        cmd = self.cmd_path
        cmd += ' -type nmds -dist %s -outdir %s' % (
            self.option('input'), self.option('output'))
        self.logger.info(u'运行ordination.pl程序计算Nmds')
        ordination_command = self.add_command('ordination_nmds', cmd)
        ordination_command.run()
        self.wait()
        if ordination_command.return_code == 0:
            self.logger.info(u'运行ordination.pl程序计算Nmds完成')
            self.end()
        else:
            self.set_error(u'运行ordination.pl程序计算Nmds出错')
