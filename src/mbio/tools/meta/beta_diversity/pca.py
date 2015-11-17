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
    last_modified:2015.11.17
    """

    def __init__(self, parent):
        super(PcaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "envtable", "type": "infile",
                "format": "meta.beta_diversity.env_table"},
            {"name": "pca_outdir", "type": "outfile",
                "format": "meta.beta_diversity.pca_outdir"}
            # 没有环境因子时，有样本坐标表，out权重表，主成分解释度表，
            # 有环境因子时，除上以外还有环境因子得分表和环境因子向量表
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供输入otu表')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class PcaTool(Tool):

    def __init__(self, config):
        super(PcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = 'meta/scripts/beta_diversity/ordination.pl'

    def run(self):
        """
        运行
        """
        super(PcaTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        """
        cmd = self.cmd_path
        cmd += ' -type pca -community %s -outdir %s' % (
            self.option('otutable').prop['path'], self.work_dir)
        if self.option('envtable').is_set:
            cmd += ' -pca_env T -environment %s' % self.option('envtable').prop['path']
        self.logger.info('运行ordination.pl程序计算pca')
        ordination_command = self.add_command('ordination_pca', cmd)
        ordination_command.run()
        self.wait()
        if ordination_command.return_code == 0:
            self.logger.info('运行ordination.pl程序计算pca完成')
            self.end()
        else:
            self.set_error('运行ordination.pl程序计算pca出错')
