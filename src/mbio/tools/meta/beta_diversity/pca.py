# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError
from mbio.files.meta.beta_diversity.pca_outdir import PcaOutdirFile


class PcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.18
    """

    def __init__(self, parent):
        super(PcaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "envtable", "type": "infile",
                "format": "meta.env_table"},
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
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/ordination.pl')

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
        outdir = os.path.join(self.work_dir, 'pca')
        cmd += ' -type pca -community %s -outdir %s' % (
            self.option('otutable').prop['path'], self.work_dir)
        if self.option('envtable').is_set:
            cmd += ' -pca_env T -environment %s' % self.option('envtable').prop[
                'path']
        self.logger.info('运行ordination.pl程序计算pca')

        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/R-3.2.2/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
            self.logger.info('pca计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('pca计算失败')
            self.set_error('R程序计算pca失败')
        pca_results = PcaOutdirFile()
        pca_results.set_path(outdir)
        self.linkfiles(pca_results)
        self.option('pca_outdir', self.output_dir)
        self.logger.info(self.option('pca_outdir').prop)
        self.logger.info('运行ordination.pl程序计算pca完成')
        self.end()

    def linkfiles(self, dir_obj):
        """
        整理结果到output文件夹
        :param dir_obj: 原始文件夹RdaOutdirFile对象
        """

        allfile = [dir_obj.prop['sites_file'], dir_obj.prop['ortation_file'], dir_obj.prop['PC_imp_file']]
        if dir_obj.prop['env_set']:
            allfile.append(dir_obj.prop['envfit_file'])
            allfile.append(dir_obj.prop['envfit_score_file'])
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

        # ordination_command = self.add_command('ordination_pca', cmd)
        # ordination_command.run()
        # self.wait()
        # if ordination_command.return_code == 0:
        #     pca_results = PcaOutdirFile()
        #     pca_results.set_path(outdir)
        #     sites = pca_results.prop['sites_file']
        #     ortation = pca_results.prop['ortation_file']
        #     imp = pca_results.prop['PC_imp_file']
        #     if os.path.exists(self.output_dir + sites):
        #         os.remove(self.output_dir + sites)
        #     if os.path.exists(self.output_dir + ortation):
        #         os.remove(self.output_dir + ortation)
        #     if os.path.exists(self.output_dir + imp):
        #         os.remove(self.output_dir + imp)
        #     os.link(outdir + sites, self.output_dir + sites)
        #     os.link(outdir + ortation, self.output_dir + ortation)
        #     os.link(outdir + imp, self.output_dir + imp)
        #     if self.option('envtable').is_set:
        #         env = pca_results.prop['envfit_file']
        #         env_score = pca_results.prop['envfit_score_file']
        #         if os.path.exists(self.output_dir + env):
        #             os.remove(self.output_dir + env)
        #         if os.path.exists(self.output_dir + env_score):
        #             os.remove(self.output_dir + env_score)
        #         os.link(outdir + env, self.output_dir + env)
        #         os.link(outdir + env_score, self.output_dir + env_score)
        #     self.option(pca_outdir, self.output_dir)
        #     self.logger.info(self.option(pca_outdir).prop)
        #     self.logger.info('运行ordination.pl程序计算pca完成')
        #     self.end()
        # else:
        #     self.set_error('运行ordination.pl程序计算pca出错')
