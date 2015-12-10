# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


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
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.env_table"}
        ]
        self.add_option(options)

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
        if not self.option('envtable').is_set:
            raise OptionError('必须提供环境因子表')
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
        self.logger.info("RUN")
        self.run_ordination()

    @property
    def formattable(self):
        tablepath = self.gettable()
        alllines = open(tablepath).readlines()
        if alllines[0][0] == '#':
            newtable = open(os.path.join(self.work_dir, 'temp.table'), 'w')
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
        tablepath = self.formattable
        self.logger.info(tablepath)
        cmd = self.cmd_path
        cmd += ' -type rdacca -community %s -environment %s -outdir %s' % (
            tablepath, self.option('envtable').prop['path'], self.work_dir)
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
        allfiles = self.get_filesname()
        for i in [1, 2, 3, 4]:
            newname = '_'.join(os.path.basename(allfiles[i]).split('_')[-2:])
            self.linkfile(allfiles[i], newname)
        newname = os.path.basename(allfiles[0]).split('_')[-1]
        self.linkfile(allfiles[0], newname)
        self.logger.info('运行ordination.pl程序计算rda/cca完成')
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

        :return rda_imp,rda_spe,rda_dca,rda_site,rda_env: 返回各个文件
        """
        filelist = os.listdir(self.work_dir + '/rda')
        rda_imp = None
        rda_spe = None
        rda_dca = None
        rda_site = None
        rda_env = None
        for name in filelist:
            if '_importance.xls' in name:
                rda_imp = name
            elif '_sites.xls' in name:
                rda_site = name
            elif '_species.xls' in name:
                rda_spe = name
            elif '_dca.txt' in name:
                rda_dca = name
            elif '_environment.xls' in name:
                rda_env = name
        if rda_imp and rda_site and rda_spe and rda_dca and rda_env:
            return [self.work_dir + '/rda/' + rda_dca,
                    self.work_dir + '/rda/' + rda_imp,
                    self.work_dir + '/rda/' + rda_spe,
                    self.work_dir + '/rda/' + rda_site,
                    self.work_dir + '/rda/' + rda_env]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
