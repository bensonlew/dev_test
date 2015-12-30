# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


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
                "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.step.add_steps('PCOA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.PCOA.start()
        self.step.update()

    def step_end(self):
        self.step.PCOA.finish()
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
        allfile = self.get_filesname()
        self.linkfile(allfile[0], 'pcoa_importance.xls')
        self.linkfile(allfile[1], 'pcoa_sites.xls')
        self.linkfile(allfile[2], 'pcoa_rotation.xls')
        self.logger.info('运行ordination.pl程序计算pcoa完成')
        self.end()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在

        :return pca_importance_file,pca_rotation_file,pca_sites_file: 返回各个文件
        """
        filelist = os.listdir(self.work_dir + '/pcoa')
        pcoa_importance_file = None
        pcoa_rotation_file = None
        pcoa_sites_file = None
        for name in filelist:
            if 'pcoa_importance.xls' in name:
                pcoa_importance_file = name
            elif 'pcoa_sites.xls' in name:
                pcoa_sites_file = name
            elif 'pcoa_rotation.xls' in name:
                pcoa_rotation_file = name
        if pcoa_importance_file and pcoa_rotation_file and pcoa_sites_file:
            return [self.work_dir + '/pcoa/' + pcoa_importance_file,
                    self.work_dir + '/pcoa/' + pcoa_rotation_file,
                    self.work_dir + '/pcoa/' + pcoa_sites_file]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
