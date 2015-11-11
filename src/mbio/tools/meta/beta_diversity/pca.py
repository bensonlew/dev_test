# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class PcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.5
    """

    def __init__(self, parent):
        super(PcaAgent, self).__init__(parent)
        options = [
            {"name": "input1", "type": "infile", "format": "otu_table"},
            # 输入文件otu表
            {"name": "input2", "type": "infile", "format": "env_table"},
            # 输入文件，可选，环境因子表
            {"name": "output", "type": "outfile", "format": "pca_outdir"}
            # 没有环境因子时，有样本坐标表，out权重表，主成分解释度表，
            # 有环境因子时，除上以外还有环境因子得分表和环境因子向量表
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError(u'必须提供输入otu表')
        if not self.option('output').is_set:
            raise OptionError(u'必须指定输出文件夹')  # 没有检查是不是文件夹

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 需要资源数暂时不清楚
        self._memory = ''  #


class PcaTool(Tool):

    def __init__(self, config):
        super(PcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = 'meta/ordination.pl'  # 暂不确定

    def run(self):
        """
        运行
        :return:
        """
        super(PcaTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        :return:
        """
        cmd = self.cmd_path
        if self.option('input2').is_set:
            cmd += ' -type pca -pca_env T -community \
                    %s -environment %s -outdir %s' % (
                self.option('input1'), self.option('input2'),
                self.option('output'))
        else:
            cmd += ' -type pca -community %s -outdir %s' % (
                self.option('input1'), self.option('output'))
        self.logger.info(u'运行ordination.pl程序计算pca')
        ordination_command = self.add_command('ordination_pca', cmd)
        ordination_command.run()
        self.wait()
        if ordination_command.return_code == 0:
            self.logger.info(u'运行ordination.pl程序计算pca完成')
            self.end()
        else:
            self.set_error(u'运行ordination.pl程序计算pca出错')
