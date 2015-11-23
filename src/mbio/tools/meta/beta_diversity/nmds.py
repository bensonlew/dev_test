# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError
from mbio.files.meta.beta_diversity.nmds_outdir import NmdsOutdirFile


class NmdsAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.18
    """

    def __init__(self, parent):
        super(NmdsAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
             "format": "meta.beta_diversity.distance_matrix"},
            {"name": "nmds_outdir", "type": "outfile",
             "format": "meta.beta_diversity.nmds_outdir"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供输入距离矩阵表')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class NmdsTool(Tool):

    def __init__(self, config):
        super(NmdsTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/ordination.pl')

    def run(self):
        """
        运行
        """
        super(NmdsTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        """
        cmd = self.cmd_path
        cmd += ' -type nmds -dist %s -outdir %s' % (
            self.option('dis_matrix').prop['path'], self.work_dir)
        self.logger.info('运行ordination.pl程序计算Nmds')
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/R-3.2.2/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('nmds计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('nmds计算失败')
            self.set_error('R程序计算nmds失败')
        nmds_results = NmdsOutdirFile()
        nmds_results.set_path(self.work_dir + '/nmds')
        sites = nmds_results.prop['sites_file']
        linksites = os.path.join(self.output_dir, os.path.basename(sites))
        if os.path.exists(linksites):
            os.remove(linksites)
        os.link(sites, linksites)
        self.option('nmds_outdir', self.output_dir)
        self.logger.info(self.option('nmds_outdir').prop)
        self.logger.info('运行ordination.pl程序计算nmds完成')
        self.end()

