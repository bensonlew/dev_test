# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
import subprocess
from biocluster.core.exceptions import OptionError


class PcaAgent(Agent):
    """
    脚本ordination.pl
    version v1.0
    author: shenghe
    last_modified:2016.3.24
    """

    def __init__(self, parent):
        super(PcaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}
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
        self.option('otutable').get_info()
        if self.option('otutable').prop['sample_num'] < 3:
            raise OptionError('otu表的样本数目少于3，不可进行beta多元分析')
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if self.option('envlabs'):
                labs = self.option('envlabs').split(',')
                for lab in labs:
                    if lab not in self.option('envtable').prop['group_scheme']:
                        raise OptionError('提供的envlabs中有不在环境因子表中存在的因子：%s' % lab)
            else:
                pass
            if self.option('envtable').prop['sample_number'] < 3:
                raise OptionError('环境因子表的样本数目少于3，不可进行beta多元分析')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            common_samples = set(samplelist) & set(self.option('envtable').prop['sample'])
            if len(common_samples) < 3:
                raise OptionError("环境因子表和OTU表的共有样本数必需大于等于3个：{}".format(len(common_samples)))
            # if len(self.option('envtable').prop['sample']) > len(samplelist):
            #     # raise OptionError('OTU表中的样本数量:%s少于环境因子表中的样本数量:%s' % (len(samplelist),
            #                     #   len(self.option('envtable').prop['sample'])))
            #     pass
            # for sample in self.option('envtable').prop['sample']:
            #     if sample not in samplelist:
            #         raise OptionError('环境因子中存在，OTU表中的未知样本%s' % sample)
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
            [".", "", "PCA分析结果输出目录"],
            ["./pca_importance.xls", "xls", "主成分解释度表"],
            ["./pca_rotation.xls", "xls", "物种主成分贡献度表"],
            ["./pca_sites.xls", "xls", "样本坐标表"],
            ["./pca_envfit_factor_scores.xls", "xls", "哑变量环境因子表"],
            ["./pca_envfit_factor.xls", "xls", "哑变量环境因子坐标表"],
            ["./pca_envfit_vector_scores.xls", "xls", "数量型环境因子表"],
            ["./pca_envfit_vector.xls", "xls", "数量型环境因子坐标表"],
        ])
        # print self.get_upload_files()
        super(PcaAgent, self).end()


class PcaTool(Tool):  # PCA需要第一行开头没有'#'的OTU表，filter_otu_sample函数生成的表头没有'#'
    def __init__(self, config):
        super(PcaTool, self).__init__(config)
        self._version = '1.0.1'  # ordination.pl脚本中指定的版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'bioinfo/statistical/scripts/ordination.pl')


    def create_otu_and_env_common(self, T1, T2, new_T1, new_T2):
        import pandas as pd
        T1 = pd.read_table(T1, sep='\t')
        T2 = pd.read_table(T2, sep='\t')
        T1_names = list(T1.columns[1:])
        T2_names = list(T2.iloc[0:, 0])
        T1_T2 = set(T1_names) - set(T2_names)
        T2_T1 = set(T2_names) - set(T1_names)
        T1T2 = set(T2_names) & set(T1_names)
        if len(T1T2) < 3:
            return False
        [T1_names.remove(value) for value in T1_T2]
        T1.to_csv(new_T1, sep="\t", columns=[T1.columns[0]] + T1_names, index=False)
        indexs = [T2_names.index(one) for one in T2_T1]
        T2 = T2.drop(indexs)
        T2.to_csv(new_T2, sep="\t", index=False)
        return True

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
        return otu_path
        # if self.option('envtable').is_set:
        #     return self.filter_otu_sample(otu_path, self.option('envtable').prop['sample'],
        #                                   os.path.join(self.work_dir, 'temp_filter.otutable'))
        # else:
        #     return otu_path

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

    def run(self):
        """
        运行
        """
        super(PcaTool, self).run()
        self.run_ordination()

    def formattable(self, tablepath):
        with open(tablepath) as table:
            if table.read(1) == '#':
                newtable = os.path.join(self.work_dir, 'temp_format.table')
                with open(newtable, 'w') as w:
                    w.write(table.read())
                return newtable
        return tablepath

    def run_ordination(self):
        """
        运行ordination.pl
        """
        old_otu_table = self.get_otu_table()
        if self.option('envtable').is_set:
            old_env_table = self.get_new_env()
            self.otu_table = self.work_dir + '/new_otu.xls'
            self.env_table = self.work_dir + '/new_env.xls'
            if not self.create_otu_and_env_common(old_otu_table, old_env_table, self.otu_table, self.env_table):
                self.set_error('环境因子表中的样本与OTU表中的样本共有数量少于2个')
        else:
            self.otu_table = old_otu_table
        real_otu_path = self.formattable(self.otu_table)
        cmd = self.cmd_path
        cmd += ' -type pca -community %s -outdir %s' % (
            real_otu_path, self.work_dir)
        if self.option('envtable').is_set:
            cmd += ' -pca_env T -environment %s' % self.env_table
        self.logger.info('运行ordination.pl程序计算pca')
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 cmd.r 文件失败')
            self.set_error('无法生成 cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/program/R-3.3.1/bin/R --restore --no-save < %s/cmd.r' % self.work_dir, shell=True)
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
            if allfiles[3]:
                self.linkfile(self.work_dir + '/pca/' + allfiles[3], 'pca_envfit_factor_scores.xls')
                self.linkfile(self.work_dir + '/pca/' + allfiles[4], 'pca_envfit_factor.xls')
            if allfiles[5]:
                self._magnify_vector(self.work_dir + '/pca/' + allfiles[5], self.work_dir + '/pca/' + allfiles[2],
                                     self.work_dir + '/pca/' + 'pca_envfit_vector_scores_magnify.xls')
                self.linkfile(self.work_dir + '/pca/' + 'pca_envfit_vector_scores_magnify.xls', 'pca_envfit_vector_scores.xls')
                self.linkfile(self.work_dir + '/pca/' + allfiles[6], 'pca_envfit_vector.xls')
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

        :return pca_importance_file, pca_rotation_file,
                pca_sites_file, pca_factor_score_file, pca_factor_file,
                pca_vector_score_file, pca_vector_file: 返回各个文件，以及是否存在环境因子，
                存在则返回环境因子结果
        """
        filelist = os.listdir(self.work_dir + '/pca')
        pca_importance_file = None
        pca_rotation_file = None
        pca_sites_file = None
        pca_factor_score_file = None
        pca_factor_file = None
        pca_vector_score_file = None
        pca_vector_file = None
        for name in filelist:
            if 'pca_importance.xls' in name:
                pca_importance_file = name
            elif 'pca_sites.xls' in name:
                pca_sites_file = name
            elif 'pca_rotation.xls' in name:
                pca_rotation_file = name
            elif 'pca_envfit_factor_scores.xls' in name:
                pca_factor_score_file = name
            elif 'pca_envfit_factor.xls' in name:
                pca_factor_file = name
            elif 'pca_envfit_vector_scores.xls' in name:
                pca_vector_score_file = name
            elif 'pca_envfit_vector.xls' in name:
                pca_vector_file = name
        if pca_importance_file and pca_rotation_file and pca_sites_file:
            if self.option('envtable').is_set:
                if pca_factor_score_file:
                    if not pca_factor_file:
                        self.set_error('未知原因，环境因子相关结果丢失或者未生成,factor文件不存在')
                else:
                    if pca_factor_file:
                        self.set_error('未知原因，环境因子相关结果丢失或者未生成,factor_scores文件不存在')
                if pca_vector_score_file:
                    if not pca_vector_file:
                        self.set_error('未知原因，环境因子相关结果丢失或者未生成,vector文件不存在')
                else:
                    if pca_vector_file:
                        self.set_error('未知原因，环境因子相关结果丢失或者未生成,vector_scores文件不存在')
                    elif not pca_factor_score_file:
                        self.set_error('未知原因，环境因子相关结果全部丢失或者未生成')
                return [pca_importance_file, pca_rotation_file,
                        pca_sites_file, pca_factor_score_file, pca_factor_file,
                        pca_vector_score_file, pca_vector_file]

            else:
                return [pca_importance_file, pca_rotation_file, pca_sites_file]
        else:
            self.set_error('未知原因，数据计算结果丢失或者未生成')
