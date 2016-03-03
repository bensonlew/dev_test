# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
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
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}
        ]
        self.add_option(options)
        self.step.add_steps('RDA_CCA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.RDA_CCA.start()
        self.step.update()

    def step_end(self):
        self.step.RDA_CCA.finish()
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

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + '/temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            self.option('envtable').set_path(new_path)
            self.option('envtable').get_info()
        else:
            self.option('envlabs', ','.join(open(self.option('envtable').path, 'r').readline().strip().split('\t')[1:]))

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        if self.option('otutable').prop['sample_num'] < 2:
            raise OptionError('otu表的样本数目少于2，不可进行beta多元分析')
        if self.option('envtable').is_set:
            if self.option('envtable').prop['sample_number'] < 2:
                raise OptionError('环境因子表的样本数目少于2，不可进行beta多元分析')
            filter_otu = self.filter_otu_sample(self.option('otutable').path,
                                                self.option('envtable').prop['sample'],
                                                os.path.join(self.work_dir + '/temp_filter.otutable'))
            if filter_otu == self.option('otutable').path:
                pass
            else:
                self.option('otutable').set_path(filter_otu)
                self.option('otutable').get_info()
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if not self.option('envtable').is_set:
            raise OptionError('必须提供环境因子表')
        else:
            self.get_new_env()
            if len(self.option('envtable').prop['sample']) != len(samplelist):
                raise OptionError('OTU表中的样本数量:%s与环境因子表中的样本数量:%s不一致' % (len(samplelist),
                                  len(self.option('envtable').prop['sample'])))
            for sample in self.option('envtable').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('环境因子中存在，OTU表中的未知样本:%s' % sample)
        table = open(self.gettable())
        if len(table.readlines()) < 4:
            raise OptionError('提供的数据表信息少于3行')
        table.close()
        return True

    def filter_otu_sample(self, otu_path, filter_samples, newfile):
        if not isinstance(filter_samples, types.ListType):
            raise Exception('过滤otu表样本的样本名称应为列表')
        try:
            with open(otu_path, 'rb') as f, open(newfile, 'wb') as w:
                one_line = f.readline()
                all_samples = one_line.rstrip().split('\t')[1:]
                if not ((set(all_samples) & set(filter_samples)) == set(filter_samples)):
                    raise Exception('提供的过滤样本存在otu表中不存在的样本all:%s,filter_samples:%s' % (all_samples, filter_samples))
                if len(all_samples) == len(filter_samples):
                    return otu_path
                samples_index = [all_samples.index(i) + 1 for i in filter_samples]
                w.write('#OTU\t' + '\t'.join(filter_samples) + '\n')
                for line in f:
                    all_values = line.rstrip().split('\t')
                    new_values = [all_values[0]] + [all_values[i] for i in samples_index]
                    w.write('\t'.join(new_values) + '\n')
                return newfile
        except IOError:
            raise Exception('无法打开OTU相关文件或者文件不存在')

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
        self.logger.info(' + '.join(self.option('envlabs').split(',')))
        cmd += ' -type rdacca -community %s -environment %s -outdir %s -env_labs %s' % (
               tablepath, self.option('envtable').prop['path'],
               self.work_dir, '+'.join(self.option('envlabs').split(',')))
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
        for i in [1, 2, 3, 4, 5]:
            if allfiles[i]:
                newname = '_'.join(os.path.basename(allfiles[i]).split('_')[-2:])
                self.linkfile(self.work_dir + '/rda/' + allfiles[i], newname)
        newname = os.path.basename(allfiles[0]).split('_')[-1]
        self.linkfile(self.work_dir + '/rda/' + allfiles[0], newname)
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

        :return rda_imp,rda_spe,rda_dca,rda_site,rda_biplot: 返回各个文件
        """
        filelist = os.listdir(self.work_dir + '/rda')
        rda_imp = None
        rda_spe = None
        rda_dca = None
        rda_site = None
        rda_biplot = None
        rda_centroids = None
        for name in filelist:
            if '_importance.xls' in name:
                rda_imp = name
            elif '_sites.xls' in name:
                rda_site = name
            elif '_species.xls' in name:
                rda_spe = name
            elif '_dca.xls' in name:
                rda_dca = name
            elif '_biplot.xls' in name:
                rda_biplot = name
            elif '_centroids' in name:
                rda_centroids = name
        if rda_imp and rda_site and rda_spe and rda_dca and (rda_biplot or rda_centroids):
            self.logger.info(str([rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids]))
            return [rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
