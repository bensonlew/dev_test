# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError
from mbio.files.meta.beta_diversity.rda_outdir import RdaOutdirFile


class RdaCcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.18
    """

    def __init__(self, parent):
        super(RdaCcaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "envtable", "type": "infile", "format": "meta.env_table"},
            {"name": "rda_outdir", "type": "outfile", "format": "meta.beta_diversity.rda_outdir"}
            # 包括样本，物种和环境因子坐标和RDA 或者CCA轴权重值
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        if not self.option('envtable').is_set:
            raise OptionError('必须提供环境因子表')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class RdaCcaTool(Tool):

    def __init__(self, config):
        super(RdaCcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/ordination.pl')

    def run(self):
        """
        运行
        """
        super(RdaCcaTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        """
        cmd = self.cmd_path
        cmd += ' -type rdacca -community %s -environment %s -outdir %s' % (
            self.option('otutable').prop['path'], self.option('envtable').prop['path'], self.work_dir)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/R-3.2.2/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('Rda/Cca计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('Rda/Cca计算失败')
            self.set_error('R程序计算Rda/Cca失败')
        rda_results = RdaOutdirFile()
        rda_results.set_path(self.work_dir + '/rda')
        self.linkfiles(rda_results)
        self.option('rda_outdir', self.output_dir)
        self.logger.info('运行ordination.pl程序计算rda/cca完成')
        self.end()

    def linkfiles(self, dir_obj):
        """
        整理结果到output文件夹
        :param dir_obj: 原始文件夹RdaOutdirFile对象
        """
        allfile = [dir_obj.prop['sites_file'], dir_obj.prop['species_file'], dir_obj.prop['imp_file'],
                   dir_obj.prop['dca_file'], dir_obj.prop['env_file']]
        for afile in allfile:
            self.linkfile(afile)

    def linkfile(self, linkfile):
        """
        连接一个文件到output
        :param linkfile: 文件路径
        """
        newlink = os.path.join(self.output_dir, os.path.basename(linkfile))
        if os.path.exists(newlink):
            os.remove(newlink)
        os.link(linkfile, newlink)
