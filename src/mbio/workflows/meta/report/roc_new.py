# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

"""个性化ROC分析"""

import os
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow
import pandas as pd


class RocNewWorkflow(Workflow):
    """
    涉及lefse分析、两组比较分析和随机森林分析以及roc分析
    version v1.0
    author: zhouxuan
    last_modify: 2017.05.12
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RocNewWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "lefse_analysis", "type": "bool", 'default': False},  # 判断是否做这三个分析
            {"name": "two_group_analysis", "type": "bool", 'default': False},
            {"name": "ran_for_analysis", "type": "bool", 'default': False},

            {"name": "lefse_otu", "type": "infile", 'format': "meta.otu.otu_table"},  # lefse 分析的参数设置
            {"name": "lefse_group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string"},
            {"name": "second_group_detail", "type": "string"},
            {"name": "lda_filter", "type": "float", "default": 2.0},
            {"name": "strict", "type": "int", "default": 0},
            {"name": "group_name", "type": "string"},
            {"name": "start_level", "type": "int", "default": 9},
            {"name": "end_level", "type": "int", "default": 9},

            {"name": "two_ran_otu", "type": "infile", 'format': "meta.otu.otu_table"},  # 两组和随机森林的参数设置
            {"name": "two_ran_group", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "level_id", "type": "int"},
            {"name": "method_cal", "type": "string", "default": "student"},
            {"name": "ci", "type": "float", "default": 0.99},
            {"name": "q_test", "type": "string", "default": "fdr"},
            {"name": "tree_number", "type": "int", "default": 500},

            {"name": "lefse_cho", "type": "string", "default": "p-value"},  # 筛选条件设置
            {"name": "lefse_num", "type": "int", "default": 50},
            {"name": "two_group_cho", "type": "string", "default": "p-value"},
            {"name": "two_group_num", "type": "int", "default": 50},
            {"name": "Ran_for_num", "type": "int", "default": 50},
            {"name": "intersection", "type": "bool", "default": False},

            {"name": "roc_calc_method", "type": "string", "default": "sum"},  # ROC计算方法以及env表的计算方法
            {"name": "roc_method_1", "type": "string", "default": ""},
            {"name": "roc_method_2", "type": "string", "default": ""},
            {"name": "env_table", "type": "infile", "format": "meta.otu.group_table"},

            {"name": "update_info", "type": "string"},  # workflow更新
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.two_ran_lefse = self.add_tool('meta.beta_diversity.roc_new')
        self.roc_lefse = self.add_tool('meta.beta_diversity.roc')
        self.roc_ran = self.add_tool('meta.beta_diversity.roc')
        self.roc_two = self.add_tool('meta.beta_diversity.roc')
        self.roc_intersection = self.add_tool('meta.beta_diversity.roc')
        self.roc_env = self.add_tool('meta.beta_diversity.roc')

    def check_options(self):
        if self.option('method_cal') not in ['student', 'welch', 'wilcox']:
            raise OptionError('错误的两组差异检验方法：%s' % self.option('method_cal'))
        if self.option('q_test') not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr", "none"]:
            raise OptionError('错误的多重检验矫正方法：%s' % self.option('q_test'))
        if self.option('lefse_cho') not in ['p-value', 'LDA']:
            raise OptionError('错误的lefse筛选方法：%s' % self.option('lefse_cho'))
        if self.option('two_group_cho') not in ['p-value', 'Corrected-p-value']:
            raise OptionError('错误的两组差异比较结果筛选方法：%s' % self.option('two_group_cho'))
        if self.option('roc_calc_method') not in ['sum', 'average', 'median', 'MI']:
            raise OptionError('错误的roc计算方法：%s' % self.option('roc_calc_method'))
        if self.option('roc_calc_method') == "MI" and (self.option('roc_method_1') == "" or self.option('roc_method_2') == ""):
            raise OptionError('错误的roc计算方法参数：%s %s' % (self.option('roc_method_1'), self.option('roc_method_2')))

    def run(self):
        self.run_two_ran_lefse()
        super(RocNewWorkflow, self).run()

    def run_two_ran_lefse(self):
        if self.option('intersection'):
            start_level = self.option("level_id")
            end_level = self.option("level_id")
        else:
            start_level = self.option("start_level")
            end_level = self.option("end_level")
        options = {
            "lefse_input": self.option("lefse_otu"),
            "lefse_group": self.option("lefse_group"),
            "otutable": self.option("two_ran_otu"),
            "grouptable": self.option("two_ran_group"),
            "lda_filter": self.option("lda_filter"),
            "strict": self.option("strict"),
            "lefse_gname": self.option("group_name"),
            "start_level": start_level,
            "end_level": end_level,
            "method_cal": self.option("method_cal"),
            "ci": self.option("ci"),
            "q_test": self.option("q_test"),
            "tree_number": self.option("tree_number"),
        }
        self.two_ran_lefse.set_options(options)
        if self.option('intersection'):
            self.two_ran_lefse.on("end", self.run_roc_intersection)
        elif self.option('lefse_analysis'):
            self.two_ran_lefse.on("end", self.run_roc_lefse)
        elif self.option('two_group_analysis'):
            self.two_ran_lefse.on("end", self.run_roc_two)
        elif self.option('ran_for_analysis'):
            self.two_ran_lefse.on("end", self.run_roc_ran)
        elif os.path.exists(self.option("env_table").prop['path']):
            self.two_ran_lefse.on("end", self.run_roc_env)
        else:
            raise Exception('参数不正确，无法正常进行roc分析')
        self.two_ran_lefse.run()

    def run_roc_intersection(self):
        fin_otu = self.get_intersection_otu()
        #new_table = self.change_otuname(fin_otu)
        options = {
            'otu_table': fin_otu,
            'level': 9,
            'group_table': self.option('two_ran_group'),
            'method': self.option('roc_calc_method'),
        }
        self.roc_intersection.set_options(options)
        if os.path.exists(self.option("env_table").prop['path']):
            self.roc_intersection.on("end", self.run_roc_env)
        else:
            self.roc_intersection.on("end", self.set_db)
        self.roc_intersection.run()

    def run_roc_env(self):
        options = {
            'otu_table': self.option("env_table").prop['path'],
            # 'level': self.option('level_id'),
            'group_table': self.option('group_table'),
            'method': self.option('method'),
        }
        self.roc_env.set_options(options)
        self.roc_env.on("end", self.set_db)
        self.roc_env.run()

    def run_roc_lefse(self):
        lefse_otu = self.get_otu_table(name="lefse")
        new_table = self.change_otuname(lefse_otu)
        options = {
            'otu_table': new_table,
            'level': self.option('level_id'),
            'group_table': self.option('group_table'),
            'method': self.option('method'),
        }
        self.roc_lefse.set_options(options)
        if self.option('two_group_analysis'):
            self.roc_lefse.on("end", self.run_roc_two)
        elif self.option('ran_for_analysis'):
            self.roc_lefse.on("end", self.run_roc_ran)
        elif os.path.exists(self.option("env_table").prop['path']):
            self.roc_lefse.on("end", self.run_roc_env)
        else:
            self.roc_lefse.on("end", self.set_db)
        self.roc_lefse.run()

    def run_roc_two(self):
        two_otu = self.get_otu_table(name="two_group")
        new_table = self.change_otuname(two_otu)
        options = {
            'otu_table': new_table,
            'level': self.option('level_id'),
            'group_table': self.option('group_table'),
            'method': self.option('method'),
        }
        self.roc_two.set_options(options)
        if self.option('ran_for_analysis'):
            self.roc_two.on("end", self.run_roc_ran)
        elif os.path.exists(self.option("env_table").prop['path']):
            self.roc_two.on("end", self.run_roc_env)
        else:
            self.roc_two.on("end", self.set_db)
        self.roc_two.run()

    def run_roc_ran(self):
        ran_otu = self.get_otu_table(name="random_forest")
        new_table = self.change_otuname(ran_otu)
        options = {
            'otu_table': new_table,
            'level': self.option('level_id'),
            'group_table': self.option('group_table'),
            'method': self.option('method'),
        }
        self.roc_ran.set_options(options)
        if os.path.exists(self.option("env_table").prop['path']):
            self.roc_two.on("end", self.run_roc_env)
        else:
            self.roc_two.on("end", self.set_db)
        self.roc_ran.run()

    def get_otu_table(self, name=None):
        import pandas as pd
        if name == "lefse":
            file_path = os.path.join(self.two_ran_lefse.output_dir, "lefse_LDA.xls")
            con = pd.read_table(file_path, header=0, sep="\t")
            con = con[con["pvalue"] != "-"]
            if self.option('lefse_cho') == "p-value":
                con = con.sort_values(by="pvalue", ascending=False)
            else:
                con = con.sort_values(by="lda", ascending=False)
            self.logger.info("lefse排序结果:{}".format(con))
            con = con.iloc[:self.option('lefse_num')]
            lefse_spe_list = [i for i in con['taxon']]  # lda表中的物种
            ori_otu = pd.read_table(os.path.join(self.two_ran_lefse.output_dir, "lefse_otu_table"), header=0, sep="\t")
            spe_list = [i for i in ori_otu['OTU ID']]  # 原始otu表中的物种
            ori_otu.index = spe_list
            index = set(lefse_spe_list) & set(spe_list)
            index_list = [i for i in index]
            # 重置index的物种应该为交集
            new_otu = ori_otu.reindex(index=index_list)  # 重置筛选物种
            table = os.path.join(self.work_dir, 'lefse_roc_input_otu.xls')
            new_otu.to_csv(table, sep="\t", index=False)
        if name == "two_group":
            file_path = os.path.join(self.two_ran_lefse.output_dir, "two_group_table.xls")
            con = pd.read_table(file_path, header=0, sep="\t")
            if self.option('two_group_cho') == "p-value":
                con = con.sort_values(by="pvalue", ascending=False)
            else:
                con = con.sort_values(by="corrected_pvalue", ascending=False)
            con = con.iloc[:self.option('two_group_num')]
            two_group_spe_list = [i for i in con[' ']]
            ori_otu = pd.read_table(self.option('two_ran_otu').prop['path'], header=0, sep="\t")
            spe_list = [i for i in ori_otu['OTU ID']]
            ori_otu.index = spe_list
            new_otu = ori_otu.reindex(index=two_group_spe_list)
            table = os.path.join(self.work_dir, 'two_group_otu.xls')
            new_otu.to_csv(table, sep="\t", index=False)
            self.logger.info("生成还没寻找各物种在哪个组中为高丰度物种的otu表格_two_group")
        if name == "random_forest":
            file_path = os.path.join(self.two_ran_lefse.output_dir, "Random_table.xls")
            con = pd.read_table(file_path, header=0, sep="\t")
            con = con.sort_values(by="MeanDecreaseAccuracy", ascending=False)
            con = con.iloc[:self.option('Ran_for_num')]
            index = con.index
            ori_otu = pd.read_table(self.option('two_ran_otu').prop['path'], header=0, sep="\t")
            spe_list = [i.split(";")[-1] for i in ori_otu['OTU ID']]
            ori_otu.index = spe_list
            new_otu = ori_otu.reindex(index=index)
            table = os.path.join(self.work_dir, 'random_forest_otu.xls')
            new_otu.to_csv(table, sep="\t", index=False)
            self.logger.info("生成还没寻找各物种在哪个组中为高丰度物种的otu表格_random_forest")
        return table

    def get_intersection_otu(self):
        path =[]
        if self.option('lefse_analysis'):
            table_lefse = self.get_otu_table(name="lefse")
            path.append(table_lefse)
        if self.option('two_group_analysis'):
            table_two = self.get_otu_table(name="two_group")
            path.append(table_two)
        if self.option('ran_for_analysis'):
            table_ran = self.get_otu_table(name="random_forest")
            path.append(table_ran)
        if len(path) == 1:
            table = path[0]
        else:
            T = 0
            new = set([])
            for i in path:
                T += 1
                con = pd.read_table(i, header=0, sep="\t")
                self.logger.info('{}的otu-id{}'.format(i, con['OTU ID']))
                spe_list = [m.split(";")[-1] for m in con['OTU ID']]  # 取最后一个分类水平的名称
                old = new
                new = set(spe_list)
                if T >= 2:
                    new = old & new
            all_spe = [i for i in new]
            if self.option('two_group_analysis'):
                any_one_con = pd.read_table(table_two, header=0, sep="\t")
            else:
                any_one_con = pd.read_table(table_ran, header=0, sep="\t")
            spe_list = [i.split(";")[-1] for i in any_one_con['OTU ID']]
            any_one_con.index = spe_list
            new_otu = any_one_con.reindex(index=all_spe)
            table = os.path.join(self.work_dir, 'intersection_otu.xls')
            new_otu.to_csv(table, sep="\t", index=False)
        return table

    def end(self):
        # result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #     [".", "", "LEfSe差异分析结果目录"],
        #     ["./lefse_LDA.cladogram.png", "png", "LEfSe分析cladogram结果图片"],
        #     ["./lefse_LDA.png", "png", "LEfSe分析LDA图片"],
        #     ["./lefse_LDA.xls", "xls", "LEfSe分析lda数据表"]
        # ])
        super(RocNewWorkflow, self).end()

    def change_otuname(self, tablepath):
        newtable = os.path.join(tablepath, 'change_otu_name')
        f2 = open(newtable, 'w+')
        with open(tablepath, 'r') as f:
            i = 0
            for line in f:
                if i == 0:
                    i = 1
                    f2.write(line)
                else:
                    line = line.strip().split('\t')
                    line_data = line[0].strip().split(' ')
                    line_he = "".join(line_data)
                    line[0] = line_he
                    # line[0] = line_data[-1]
                    for i in range(0, len(line)):
                        if i == len(line) - 1:
                            f2.write("%s\n" % (line[i]))
                        else:
                            f2.write("%s\t" % (line[i]))
        f2.close()
        return newtable

    def set_db(self):
        """
        保存两组比较分析的结果表保存到mongo数据库中
        """
        # api_lefse = self.api.stat_test
        self.end()
