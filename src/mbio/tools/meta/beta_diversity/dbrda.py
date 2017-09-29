# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
# import os
import types
import os
from biocluster.core.exceptions import OptionError
from mbio.files.meta.otu.otu_table import OtuTableFile
from mbio.packages.beta_diversity.dbrda_r import *


class DbrdaAgent(Agent):
    """
    dbrda_r.py
    version v1.0
    author: shenghe
    last_modified:2016.03.24
    """
    # METHOD = ["manhattan", "euclidean", "canberra", "bray", "kulczynski", "jaccard", "gower", "morisita", "horn",
    #           "mountford", "raup", "binomial", "chao"]
    METHOD = ["manhattan", "euclidean", "canberra", "bray", "kulczynski", "gower"]
    METHOD_DICT = {"manhattan": "manhattan", "euclidean": "euclidean", "canberra": "canberra",
                   "bray_curtis": "bray", "kulczynski": "kulczynski", "gower": "gower"}

    def __init__(self, parent):
        super(DbrdaAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "dis_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}  # 用逗号分隔的环境因子名称
        ]
        self.add_option(options)
        self.step.add_steps('dbRDA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.dbRDA.start()
        self.step.update()

    def step_end(self):
        self.step.dbRDA.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的丰度表对象，否则直接返回参数otutable对象
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            new_otu = OtuTableFile()
            new_otu.set_path(self.option('otutable').get_table(self.option('level')))
            new_otu.get_info()
            return new_otu
        else:
            return self.option('otutable')

    def check_options(self):
        """
        重写参数检查
        """
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
            if self.option('dis_matrix').is_set:
                # self.option('dis_matrix').get_info()
                env_collection = set(self.option('envtable').prop['sample'])
                collection = set(self.option('dis_matrix').prop['samp_list']) & env_collection
                if len(collection) < 3:
                    raise OptionError("环境因子表和丰度表的共有样本数必需大于等于3个：{}".format(len(collection)))
                # if collection == env_collection:  # 检查环境因子的样本是否是丰度表中样本的子集
                #     pass
                # else:
                #     raise OptionError('环境因子中存在距离矩阵中没有的样本')
                pass
            else:
                if self.option('method') not in DbrdaAgent.METHOD_DICT:
                    raise OptionError('错误或者不支持的距离计算方法')
                self.option('method', DbrdaAgent.METHOD_DICT[self.option('method')])
                if not self.option('otutable').is_set:
                    raise OptionError('没有提供距离矩阵的情况下，必须提供丰度表')
                self.real_otu = self.gettable()
                if self.real_otu.prop['sample_num'] < 3:
                    raise OptionError('丰度表的样本数目少于3，不可进行beta多元分析')
                samplelist = open(self.real_otu.path).readline().strip().split('\t')[1:]
                # if len(self.option('envtable').prop['sample']) > len(samplelist):
                #     raise OptionError('丰度表中的样本数量:%s少于环境因子表中的样本数量:%s' % (len(samplelist),
                #                       len(self.option('envtable').prop['sample'])))
                # for sample in self.option('envtable').prop['sample']:
                #     if sample not in samplelist:
                #         raise OptionError('环境因子中存在，丰度表中的未知样本%s' % sample)
                common_samples = set(samplelist) & set(self.option('envtable').prop['sample'])
                if len(common_samples) < 3:
                    raise OptionError("环境因子表和丰度表的共有样本数必需大于等于3个：{}".format(len(common_samples)))
                table = open(self.real_otu.path)
                if len(table.readlines()) < 4:
                    raise OptionError('提供的数据表信息少于3行')
                table.close()
                pass
        else:
            raise OptionError('没有提供环境因子表')
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = '3G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "db_rda分析结果目录"],
            ["./db_rda_sites.xls", "xls", "db_rda样本坐标表"],
            ["./db_rda_importance.xls", "xls", "db_rda主成分解释度"], ##add by zhujuan 2017.08.21
            ["./db_rda_species.xls", "xls", "db_rda物种坐标表"],
            ["./db_rda_centroids.xls", "xls", "db_rda哑变量环境因子坐标表"],
            ["./db_rda_biplot.xls", "xls", "db_rda数量型环境因子坐标表"],
        ])
        # print self.get_upload_files()
        super(DbrdaAgent, self).end()


class DbrdaTool(Tool):
    def __init__(self, config):
        super(DbrdaTool, self).__init__(config)
        self._version = '1.0'
        # 模块脚本路径，并不使用
        self.cmd_path = 'mbio/packages/beta_diversity/dbrda_r.py'
        self.env_table = self.get_new_env()
        if not self.option('dis_matrix').is_set:
            self.otu_table = self.get_otu_table()
            new_otu_table = self.work_dir + '/new_otu_table.xls'
            new_env_table = self.work_dir + '/new_env_table.xls'
            if not self.create_otu_and_env_common(self.otu_table, self.env_table, new_otu_table, new_env_table):
                self.set_error('环境因子表中的样本与丰度表中的样本共有数量少于2个')
            else:
                self.otu_table = new_otu_table
                self.env_table = new_env_table
        else:
            samples = list(set(self.option('dis_matrix').prop['samp_list']) & set(self.option('envtable').prop['sample']))
            self.env_table = self.sub_env(samples)
            self.dis_matrix = self.get_matrix(samples)

    def sub_env(self, samples):
        with open(self.env_table) as f, open(self.work_dir + '/sub_env_temp.xls', 'w') as w:
            w.write(f.readline())
            for i in f:
                if i.split('\t')[0] in samples:
                    w.write(i)
        return self.work_dir + '/sub_env_temp.xls'


    def create_otu_and_env_common(self, T1, T2, new_T1, new_T2):
        import pandas as pd
        T1 = pd.read_table(T1, sep='\t', dtype=str)
        T2 = pd.read_table(T2, sep='\t', dtype=str)
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

    def get_matrix(self, samples):
        if len(self.option('dis_matrix').prop['samp_list']) == len(self.option('envtable').prop['sample']):
            return self.option('dis_matrix').path
        else:
            # samples = list(set(self.option('dis_matrix').prop['samp_list']) & set(self.option('envtable').prop['sample']))
            self.option('dis_matrix').create_new(samples,
                                                 os.path.join(self.work_dir, 'dis_matrix_filter.temp'))
            return os.path.join(self.work_dir, 'dis_matrix_filter.temp')

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
        根据level返回进行计算的丰度表路径
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        # 丰度表对象没有样本列表属性
        return otu_path
        # return self.filter_otu_sample(otu_path, self.option('envtable').prop['sample'],
        #                               os.path.join(self.work_dir + 'temp_filter.otutable'))

    def filter_otu_sample(self, otu_path, filter_samples, newfile):
        if not isinstance(filter_samples, types.ListType):
            raise Exception('过滤丰度表样本的样本名称应为列表')
        try:
            with open(otu_path, 'rb') as f, open(newfile, 'wb') as w:
                one_line = f.readline()
                all_samples = one_line.rstrip().split('\t')[1:]
                if not ((set(all_samples) & set(filter_samples)) == set(filter_samples)):
                    raise Exception('提供的过滤样本存在丰度表中不存在的样本all:%s,filter_samples:%s' % (all_samples, filter_samples))
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
            raise Exception('无法打开丰度相关文件或者文件不存在')

    def run(self):
        """
        运行
        """
        super(DbrdaTool, self).run()
        self.run_dbrda()

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

    def run_dbrda(self):
        """
        运行dbrda.py
        """
        self.logger.info('运行dbrda_r.py程序计算Dbrda')
        if self.option('dis_matrix').is_set:
            return_mess = db_rda_dist(dis_matrix=self.dis_matrix, env=self.env_table,
                                      output_dir=self.work_dir)
        else:
            return_mess = db_rda_new(self.otu_table, self.env_table, self.work_dir,
                                     self.option('method'))
        # self.logger.info('运行dbrda_r.py程序计算Dbrda成功')
        if return_mess == 0:
            self.linkfile(self.work_dir + '/db_rda_sites.xls', 'db_rda_sites.xls')
            self.linkfile(self.work_dir + '/db_rda_cont.xls','db_rda_importance.xls') ##add by zhujuan 20170821
            if not self.option('dis_matrix').is_set:
                self.linkfile(self.work_dir + '/db_rda_species.xls', 'db_rda_species.xls')
            lines = open(self.work_dir + '/env_data.temp').readlines()
            if 'centroids:TRUE' in lines[0]:
                self.linkfile(self.work_dir + '/db_rda_centroids.xls', 'db_rda_centroids.xls')
            if 'biplot:TRUE' in lines[1]:
                self.linkfile(self.work_dir + '/db_rda_biplot.xls', 'db_rda_biplot.xls')
            self.logger.info('运行dbrda_r.py程序计算Dbrda完成')
            self.end()
        else:
            self.set_error('运行dbrda_r.py程序计算Dbrda出错')
