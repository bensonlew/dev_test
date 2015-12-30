# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


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
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile",
             "format": "meta.env_table"}
        ]
        self.add_option(options)
        self.step.add_steps('PCAanalysis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.PCAanalysis.start()
        self.step.update()

    def step_end(self):
        self.step.PCAanalysis.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if len(self.option('envtable').prop['sample']) != len(samplelist):
                raise OptionError('OTU表中的样本数量:%s与环境因子表中的样本数量:%s不一致' % (len(samplelist),
                    len(self.option('envtable').prop['sample'])))
            for sample in self.option('envtable').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('环境因子中存在，OTU表中的未知样本%s' % sample)
        table = open(self.gettable())
        if len(table.readlines()) < 4:
            raise OptionError('提供的数据表信息少于3行')
        table.close()
        return True

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

    def formattable(self):
        tablepath = self.gettable()
        alllines = open(tablepath).readlines()
        if alllines[0][0] == '#':
            newtable = open(os.path.join(self.work_dir, 'temp.table'),'w')
            newtable.write(alllines[0].lstrip('#'))
            for line in alllines[1:]:
                newtable.write(line)
            newtable.close()
            return os.path.join(self.work_dir, 'temp.table')
        else:
            return tablepath

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def run_ordination(self):
        """
        运行ordination.pl
        """
        tablepath = self.formattable()
        cmd = self.cmd_path
        cmd += ' -type pca -community %s -outdir %s' % (
            tablepath, self.work_dir)
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
        self.logger.info('运行ordination.pl程序计算pca完成')
        allfiles = self.get_filesname()
        self.linkfile(self.work_dir + '/pca/' + allfiles[0], 'pca_importance.xls')
        self.linkfile(self.work_dir + '/pca/' + allfiles[1], 'pca_rotation.xls')
        self.linkfile(self.work_dir + '/pca/' + allfiles[2], 'pca_sites.xls')
        if self.option('envtable').is_set:
            self.linkfile(self.work_dir + '/pca/' + allfiles[3], 'pca_envfit_score.xls')
            self.linkfile(self.work_dir + '/pca/' + allfiles[4], 'pca_envfit.xls')
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

        :return pca_importance_file, pca_rotation_file,
        pca_sites_file, env_set, pca_envfit_score_file,
        pca_envfit_file: 返回各个文件，以及是否存在环境因子，
        存在则返回环境因子结果
        """
        filelist = os.listdir(self.work_dir + '/pca')
        pca_importance_file = None
        pca_rotation_file = None
        pca_sites_file = None
        pca_envfit_score_file = None
        pca_envfit_file = None
        for name in filelist:
            if 'pca_importance.xls' in name:
                pca_importance_file = name
            elif 'pca_sites.xls' in name:
                pca_sites_file = name
            elif 'pca_rotation.xls' in name:
                pca_rotation_file = name
            elif 'pca_envfit_score.xls' in name:
                pca_envfit_score_file = name
            elif 'pca_envfit.xls' in name:
                pca_envfit_file = name
        if pca_importance_file and pca_rotation_file and pca_sites_file:
            if self.option('envtable').is_set:
                if pca_envfit_score_file and pca_envfit_file:
                    return [pca_importance_file, pca_rotation_file,
                            pca_sites_file, pca_envfit_score_file,
                            pca_envfit_file]
                else:
                    self.set_error('未知原因，环境因子相关结果丢失或者未生成')
            else:
                return [pca_importance_file, pca_rotation_file, pca_sites_file]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
