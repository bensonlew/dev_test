# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


class NmdsAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2016.3.24
    """

    def __init__(self, parent):
        super(NmdsAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
             "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.step.add_steps('NMDS')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.NMDS.start()
        self.step.update()

    def step_end(self):
        self.step.NMDS.finish()
        self.step.update()

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
        self._memory = '3G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "NMDS分析结果输出目录"],
            ["./nmds_sites.xls", "xls", "样本坐标表"],
            ["./nmds_stress.xls", "xls", "样本特征拟合度值"],
        ])
        # print self.get_upload_files()
        super(NmdsAgent, self).end()


class NmdsTool(Tool):

    def __init__(self, config):
        super(NmdsTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'bioinfo/statistical/scripts/ordination.pl')

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
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('nmds计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('nmds计算失败')
            self.set_error('R程序计算nmds失败')
            raise Exception('R程序计算nmds失败')
        filename = None
        stress = None
        for name in os.listdir(self.work_dir + '/nmds'):
            if 'nmds_sites.xls' in name:
                filename = name
            elif 'nmds_stress.xls' in name:
                stress = name
        if not filename:
            self.set_error('未知原因sites文件没有生成')
        if not stress:
            self.set_error('未知原因stress文件没有生成')
        linksites = os.path.join(self.output_dir, 'nmds_sites.xls')
        if os.path.exists(linksites):
            os.remove(linksites)
        os.link(self.work_dir + '/nmds' + '/' + filename, linksites)
        linkstress = os.path.join(self.output_dir, 'nmds_stress.xls')
        if os.path.exists(linkstress):
            os.remove(linkstress)
        os.link(self.work_dir + '/nmds' + '/' + stress, linkstress)
        self.logger.info('运行ordination.pl程序计算nmds完成')
        self.end()
