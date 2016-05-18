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
    last_modified:2016.3.24
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

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        if self.option('otutable').prop['sample_num'] < 2:
            raise OptionError('otu表的样本数目少于2，不可进行beta多元分析')
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if self.option('envlabs'):
                labs = self.option('envlabs').split(',')
                for lab in labs:
                    if lab not in self.option('envtable').prop['group_scheme']:
                        raise OptionError('提供的envlabs中有不在环境因子表中存在的因子：%s' % lab)
            else:
                pass
            if self.option('envtable').prop['sample_number'] < 2:
                raise OptionError('环境因子表的样本数目少于2，不可进行beta多元分析')
        else:
            raise OptionError('必须提供环境因子表')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if len(self.option('envtable').prop['sample']) > len(samplelist):
            raise OptionError('OTU表中的样本数量:%s小于环境因子表中的样本数量:%s' % (len(samplelist),
                              len(self.option('envtable').prop['sample'])))
        for sample in self.option('envtable').prop['sample']:
            if sample not in samplelist:
                raise OptionError('环境因子中存在，OTU表中的未知样本:%s' % sample)
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

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "rda_cca分析结果目录"]
        ])
        result_dir.add_regexp_rules([
            [r'.*_importance\.xls', 'xls', '主成分解释度表'],
            [r'.*_sites\.xls', 'xls', '样本坐标表'],
            [r'.*_species\.xls', 'xls', '物种坐标表'],
            [r'.*dca\.xls', 'xls', 'DCA分析结果'],
            [r'.*_biplot\.xls', 'xls', '数量型环境因子坐标表'],
            [r'.*_centroids\.xls', 'xls', '哑变量环境因子坐标表']
        ])
        print self.get_upload_files()
        super(RdaCcaAgent, self).end()


class RdaCcaTool(Tool):  # rda/cca需要第一行开头没有'#'的OTU表，filter_otu_sample函数生成的表头没有'#'
    def __init__(self, config):
        super(RdaCcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/ordination.pl')
        self.env_table = self.get_new_env()
        self.otu_table = self.get_otu_table()
        self.env_labs = open(self.env_table, 'r').readline().strip().split('\t')[1:]

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + '/temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            return new_path
        else:
            return self.option('envtable').path

    def get_otu_table(self):
        """
        根据level返回进行计算的otu表路径
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        # otu表对象没有样本列表属性
        return self.filter_otu_sample(otu_path, self.option('envtable').prop['sample'],
                                      os.path.join(self.work_dir, 'temp_filter.otutable'))

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
                w.write('OTU\t' + '\t'.join(filter_samples) + '\n')
                for line in f:
                    all_values = line.rstrip().split('\t')
                    new_values = [all_values[0]] + [all_values[i] for i in samples_index]
                    w.write('\t'.join(new_values) + '\n')
                return newfile
        except IOError:
            raise Exception('无法打开OTU相关文件或者文件不存在')


    def run(self):
        """
        运行
        """
        super(RdaCcaTool, self).run()
        self.logger.info("RUN")
        self.run_ordination()

    def formattable(self, tablepath):
        alllines = open(tablepath).readlines()
        if alllines[0][0] == '#':
            newtable = open(os.path.join(self.work_dir, 'temp_format.table'), 'w')
            newtable.write(alllines[0].lstrip('#'))
            newtable.writelines(alllines[1:])
            newtable.close()
            return os.path.join(self.work_dir, 'temp_format.table')
        else:
            return tablepath

    def run_ordination(self):
        """
        运行ordination.pl
        """
        tablepath = self.formattable(self.otu_table)
        self.logger.info(tablepath)
        cmd = self.cmd_path
        cmd += ' -type rdacca -community %s -environment %s -outdir %s -env_labs %s' % (
               tablepath, self.option('envtable').prop['path'],
               self.work_dir, '+'.join(self.env_labs))
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
                if i == 4:
                    self._magnify_vector(self.work_dir + '/rda/' + allfiles[4], self.work_dir + '/rda/' + allfiles[3],
                                         self.work_dir + '/rda/' + 'magnify_' + newname)
                    self.linkfile(self.work_dir + '/rda/' + 'magnify_' + newname, newname)
                else:
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

    def _magnify_vector(self, vector_file, sites_file, new_vector):
        """
        放大环境因子向量的箭头
        """
        def get_range(path):
            with open(path) as sites:
                sites.readline()
                pc1 = []
                pc2 = []
                for line in sites:
                    split_line = line.rstrip().split('\t')
                    if len(split_line) < 3:
                        self.set_error('未知原因，坐标文件少于3列')
                    pc1.append(abs(float(split_line[1])))
                    pc2.append(abs(float(split_line[2])))
                # range_pc = min([max(pc1), max(pc2)])
            return max(pc1), max(pc2)
        range_vector_pc1, range_vector_pc2 = get_range(vector_file)
        range_sites_pc1, range_sites_pc2 = get_range(sites_file)
        magnify_1 = range_sites_pc1 / range_vector_pc1
        magnify_2 = range_sites_pc2 / range_vector_pc2
        magnify = magnify_1 if magnify_1 < magnify_2 else magnify_2
        with open(new_vector, 'w') as new, open(vector_file) as vector:
            new.write(vector.readline())
            for line in vector:
                line_split = line.rstrip().split('\t')
                for i in range(len(line_split) - 1):
                    index = i + 1
                    line_split[index] = str(float(line_split[index]) * magnify)
                new.write('\t'.join(line_split) + '\n')

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
            elif 'dca.xls' in name:
                rda_dca = name
            elif '_biplot.xls' in name:
                rda_biplot = name
            elif '_centroids.xls' in name:
                rda_centroids = name
        if rda_imp and rda_site and rda_spe and rda_dca and (rda_biplot or rda_centroids):
            self.logger.info(str([rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids]))
            return [rda_dca, rda_imp, rda_spe, rda_site, rda_biplot, rda_centroids]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
