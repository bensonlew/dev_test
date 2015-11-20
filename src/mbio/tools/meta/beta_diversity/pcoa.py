# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError
from mbio.files.meta.beta_diversity.pcoa_outdir import PcoaOutdirFile


class PcoaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.18
    """

    def __init__(self, parent):
        super(PcoaAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
                "format": "meta.beta_diversity.distance_matrix"},
            {"name": "pcoa_outdir", "type": "outfile",
                "format": "meta.beta_diversity.pcoa_outdir"}
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


class PcoaTool(Tool):

    def __init__(self, config):
        super(PcoaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/ordination.pl')

    def run(self):
        """
        运行
        """
        super(PcoaTool, self).run()
        self.run_ordination()

    def run_ordination(self):
        """
        运行ordination.pl
        """
        cmd = self.cmd_path
        cmd += ' -type pcoa -dist %s -outdir %s' % (
            self.option('dis_matrix').prop['path'], self.work_dir)
        self.logger.info('运行ordination.pl程序计算pcoa')

        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/R-3.2.2/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('pcoa计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('pcoa计算失败')
            self.set_error('R程序计算pcoa失败')
        pcoa_results = PcoaOutdirFile()
        pcoa_results.set_path(self.work_dir + '/pcoa')
        self.linkfiles(pcoa_results)
        self.option('pcoa_outdir', self.output_dir)
        self.logger.info(self.option('pcoa_outdir').prop)
        self.logger.info('运行ordination.pl程序计算pcoa完成')
        self.end()

    def linkfiles(self, dir_obj):
        """
        整理结果到output文件夹
        :param dir_obj: 原始文件夹RdaOutdirFile对象
        """
        allfile = [dir_obj.prop['sites_file'], dir_obj.prop['ortation_file'], dir_obj.prop['PC_imp_file']]
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

