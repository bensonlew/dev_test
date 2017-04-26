# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.statistical.metastat import *
from mbio.packages.statistical.twogroup_CI import *
from mbio.packages.statistical.twosample_CI import *
from mbio.packages.statistical.mul_posthoc import *
import os
import re


class MetastatAgent(Agent):
    """
    statistical metastat+ 调用metastat.py 包进行差异性分析
    version v1.0
    author: qiuping
    last_modify: 2015.11.30
    """
    def __init__(self, parent):
        super(MetastatAgent, self).__init__(parent)
        options = [
            {"name": "chi_input", "type": "infile", "format": "meta.otu.otu_table"},  # 卡方检验的输入文件
            {"name": "chi_sample1", "type": "string"},  # 卡方检验的输入样品名称
            {"name": "chi_sample2", "type": "string"},  # 卡方检验的输入样品名称
            {"name": "chi_correction", "type": "string", "default": "none"},  # 卡方检验的多重检验校正
            {"name": "fisher_input", "type": "infile", "format": "meta.otu.otu_table"},  # 费舍尔检验的输入文件
            {"name": "fisher_ci", "type": "float", "default": 0.05},  # 费舍尔检验的显著性水平
            {"name": "fisher_sample1", "type": "string"},  # 费舍尔检验的输入样品名称1
            {"name": "fisher_sample2", "type": "string"},  # 费舍尔检验的输入样品名称2
            {"name": "fisher_correction", "type": "string", "default": "none"},  # 费舍尔检验的多重检验校正
            {"name": "fisher_type", "type": "string", "default": "two.side"},  # 费舍尔检验的选择单尾或双尾检验
            {"name": "kru_H_input", "type": "infile", "format": "meta.otu.otu_table"},  # kruskal_wallis_H_test的输入文件
            {"name": "kru_H_group", "type": "infile", "format": "meta.otu.group_table"},  # kruskal_wallis_H_test输入分组
            # {"name": "kru_H_type", "type": "string", "default": "two.side"},  #kruskal_wallis_H_test选择单双尾检验
            {"name": "kru_H_correction", "type": "string", "default": "none"},  # kruskal_wallis_H_test的多重检验校正
            {"name": "mann_input", "type": "infile", "format": "meta.otu.otu_table"},  # 秩和检验的输入文件
            {"name": "mann_ci", "type": "float", "default": 0.05},  # 秩和检验的显著性水平
            {"name": "mann_group", "type": "infile", "format": "meta.otu.group_table"},  # 秩和检验的输入分组文件
            {"name": "mann_correction", "type": "string", "default": "none"},  # 秩和检验的多重检验校正
            {"name": "mann_type", "type": "string", "default": "two.side"},  # 秩和检验的选择单尾或双尾检验
            {"name": "student_input", "type": "infile", "format": "meta.otu.otu_table"},  # T检验的输入文件
            {"name": "student_ci", "type": "float", "default": 0.05},  # T检验的显著性水平
            {"name": "student_group", "type": "infile", "format": "meta.otu.group_table"},  # T检验的输入分组文件
            {"name": "student_correction", "type": "string", "default": "none"},  # T检验的多重检验校正
            {"name": "student_type", "type": "string", "default": "two.side"},  # T检验的选择单尾或双尾检验
            {"name": "welch_input", "type": "infile", "format": "meta.otu.otu_table"},  # welch_T检验的输入文件
            {"name": "welch_ci", "type": "float", "default": 0.05},  # welch_T检验的显著性水平
            {"name": "welch_group", "type": "infile", "format": "meta.otu.group_table"},  # welch_T检验的输入分组文件
            {"name": "welch_correction", "type": "string", "default": "none"},  # welch_T检验的多重检验校正
            {"name": "welch_type", "type": "string", "default": "two.side"},  # welch_T检验的选择单尾或双尾检验
            {"name": "anova_input", "type": "infile", "format": "meta.otu.otu_table"},  # anova分析的输入文件
            {"name": "anova_group", "type": "infile", "format": "meta.otu.group_table"},  # anova分析的输入分组文件
            {"name": "anova_correction", "type": "string", "default": "none"},  # anova分析的多重检验校正
            {"name": "test", "type": "string"},   # 选择统计学检验分析方法
            {"name": "student_gname", "type": "string"},  # student检验分组方案选择
            {"name": "welch_gname", "type": "string"},  # welch检验分组方案选择
            {"name": "mann_gname", "type": "string"},  # wilcox秩和检验分组方案选择
            {"name": "kru_H_gname", "type": "string"},  # kru检验分组方案选择
            {"name": "anova_gname", "type": "string"},  # 单因素方差分析分组方案选择
            {"name": "kru_H_coverage", "type": "float", "default": 0.95},  # 计算置信区间所选择的置信度
            {"name": "anova_coverage", "type": "float", "default": 0.95},
            {"name": "student_coverage", "type": "float", "default": 0.95},
            {"name": "welch_coverage", "type": "float", "default": 0.95},
            {"name": "mann_coverage", "type": "float", "default": 0.95},
            {"name": "chi_coverage", "type": "float", "default": 0.95},
            {"name": "fisher_coverage", "type": "float", "default": 0.95},
            {"name": "kru_H_methor", "type": "string", "default": 'tukeykramer'},  # post-hoc检验的方法
            {"name": "anova_methor", "type": "string", "default": 'tukeykramer'},  # post-hoc检验的方法
            {"name": "chi_methor", "type": "string", "default": 'DiffBetweenPropAsymptotic'},  # 两样本计算置信区间的方法
            {"name": "fisher_methor", "type": "string", "default": 'DiffBetweenPropAsymptotic'},  # 两样本计算置信区间的方法
            {"name": "est_group", "type": "infile", "format": "meta.alpha_diversity.group_file_dir"},
            {"name": "est_input", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "est_test_method", "type": "string", "default": "student"}  # add by qindanhua 20170105
        ]
        self.add_option(options)
        self.step.add_steps("stat_test")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.stat_test.start()
        self.step.update()

    def stepfinish(self):
        self.step.stat_test.finish()
        self.step.update()

    def check_sample(self, group_sample, otu_sample):
        for name in group_sample:
            if name not in otu_sample:
                raise OptionError('分组样本不在otu表所拥有的样本内，请检查分组方案')

    def check_group(self, groupfile):
        with open(groupfile, 'rb') as g:
            info = g.readlines()
            sample = []
            group_dict = {}
            group_num = {}
            for line in info[1:]:
                sample.append(line.strip('\n').split('\t')[0])
                line = line.split()
                group_dict[line[0]] = line[1]
            gnames = group_dict.values()
            for gname in gnames:
                group_num[gname] = gnames.count(gname)
            sam_num = group_num.values()
            if 1 in sam_num:
                raise OptionError('一个分组类别下面至少有两个样本，请重新分组')
            if len(sample) != len(set(sample)):
                raise OptionError('不同的分组类别下有相同的样本，此分析不支持该分组方案，请重新分组')

    def check_options(self):
        """
        检查参数设置
        :return:
        """
        if not self.option('test'):
            self.logger.info(self.option('test'))
            raise OptionError("必须设置输入的检验名称")
        for i in self.option('test').split(','):
            if i not in ["chi", "fisher", "kru_H", "mann", "anova", "student", "welch", "estimator"]:
                raise OptionError("所输入的检验名称不对")
            elif i == "chi":
                if not self.option("chi_input").is_set:
                    raise OptionError('必须设置卡方检验输入的otutable文件')
                if not self.option("chi_sample1") and not self.option("chi_sample2"):
                    raise OptionError('必须设置卡方检验要比较的样品名')
                if self.option("chi_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr",
                                                         "none"]:
                    raise OptionError('卡方检验的多重检验校正的方法不被支持')
                if self.option("chi_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('卡方检验的置信区间的置信度不在范围值内')
                if self.option("chi_methor") not in ["DiffBetweenPropAsymptoticCC", "DiffBetweenPropAsymptotic",
                                                     "NewcombeWilson"]:
                    raise OptionError('卡方检验的计算置信区间的方法不在范围值内')
                otu_sample = self.option('chi_input').get_sample_info()
                if self.option('chi_sample1') not in otu_sample or self.option('chi_sample2') not in otu_sample:
                    raise OptionError('输入的样本不在otu表所拥有的样本内，请检查卡方检验样本名')
            elif i == "fisher":
                if not self.option("fisher_input").is_set:
                    raise OptionError('必须设置费舍尔检验输入的otutable文件')
                if not self.option("fisher_sample1") and not self.option("fisher_sample2"):
                    raise OptionError('必须设置费舍尔检验要比较的样品名')
                if self.option("fisher_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                            "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option("fisher_ci") <= 0 or self.option("fisher_ci") >= 1:
                    raise OptionError('所输入的显著水平不在范围值内')
                if self.option("fisher_type") not in ["two.side", "greater", "less"]:
                    raise OptionError('所输入的类型不在范围值内')
                if self.option("fisher_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('费舍尔检验的置信区间的置信度不在范围值内')
                if self.option("fisher_methor") not in ["DiffBetweenPropAsymptoticCC", "DiffBetweenPropAsymptotic",
                                                        "NewcombeWilson"]:
                    raise OptionError('费舍尔检验的计算置信区间的方法不在范围值内')
                otu_sample = self.option('fisher_input').get_sample_info()
                if self.option('fisher_sample1') not in otu_sample or self.option('fisher_sample2') not in otu_sample:
                    raise OptionError('输入的样本不在otu表所拥有的样本内，请检查费舍尔检验样本名')
            elif i == "kru_H":
                if not self.option("kru_H_input").is_set:
                    raise OptionError('必须设置Kruskal-Wallis秩和检验输入的otutable文件')
                if not self.option("kru_H_group").is_set:
                    raise OptionError('必须设置Kruskal-Wallis秩和检验输入的分组文件')
                if not self.option("kru_H_gname"):
                    raise OptionError("Kruskal-Wallis秩和检验分组方案选择参数为必须参数，请设置")
                if len(self.option("kru_H_gname").split(',')) != 1:
                    raise OptionError("组间差异的分组方案只能为1个")
                if self.option("kru_H_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                           "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                # if self.option("kru_H_type") not in ["two.side", "greater", "less"]:
                #     raise OptionError('所输入的类型不在范围值内')
                gnum = self.option('kru_H_group').group_num(self.option('kru_H_gname'))
                if gnum < 3:
                    raise OptionError("Kruskal-Wallis秩和检验的分组方案的分组类别必须大于等于3")
                if self.option("kru_H_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('Kruskal-Wallis秩和检验的posthoc的置信度不在范围值内')
                if self.option("kru_H_methor") not in ["scheffe", "welchuncorrected", "tukeykramer", "gameshowell"]:
                    raise OptionError('Kruskal-Wallis秩和检验的posthoc检验方法不在范围值内')
                otu_sample = self.option('kru_H_input').get_sample_info()
                self.logger.info(otu_sample)
                group_sample, header, is_empty = self.option('kru_H_group').get_file_info()
                self.check_sample(group_sample, otu_sample)
                self.check_group(self.option('kru_H_group').prop['path'])
            elif i == "anova":
                if not self.option("anova_input").is_set:
                    raise OptionError('必须设置Kruskal-Wallis秩和检验输入的otutable文件')
                if not self.option("anova_group").is_set:
                    raise OptionError('必须设置Kruskal-Wallis秩和检验输入的分组文件')
                if not self.option("anova_gname"):
                    raise OptionError("单因素方差分析分组方案选择参数为必须参数，请设置")
                if len(self.option("anova_gname").split(',')) != 1:
                    raise OptionError("组间差异的分组方案只能为1个")
                if self.option("anova_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                           "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                gnum = self.option('anova_group').group_num(self.option('anova_gname'))
                if gnum < 3:
                    raise OptionError("单因素方差分析的分组方案的分组类别必须大于等于3")
                if self.option("anova_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('单因素方差分析的posthoc的置信度不在范围值内')
                if self.option("anova_methor") not in ["scheffe", "welchuncorrected", "tukeykramer", "gameshowell"]:
                    raise OptionError('单因素方差分析的posthoc检验方法不在范围值内')
                otu_sample = self.option('anova_input').get_sample_info()
                self.logger.info(otu_sample)
                group_sample, header, is_empty = self.option('anova_group').get_file_info()
                self.check_sample(group_sample, otu_sample)
                self.check_group(self.option('anova_group').prop['path'])
            elif i == "mann":
                if not self.option("mann_input").is_set:
                    raise OptionError('必须设置wilcox秩和检验输入的otutable文件')
                if not self.option("mann_group").is_set:
                    raise OptionError('必须设置wilcox秩和检验输入的分组文件')
                if self.option("mann_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr",
                                                          "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option("mann_ci") <= 0 or self.option("mann_ci") >= 1:
                    raise OptionError('所输入的显著水平不在范围值内')
                if self.option("mann_type") not in ["two.side", "greater", "less"]:
                    raise OptionError('所输入的类型不在范围值内')
                if not self.option("mann_gname"):
                    raise OptionError("wilcox秩和检验分组方案选择参数为必须参数，请设置")
                if len(self.option("mann_gname").split(',')) != 1:
                    raise OptionError("组间差异的分组方案只能为1个")
                gnum = self.option('mann_group').group_num(self.option('mann_gname'))
                if gnum != 2:
                    raise OptionError("wilcox秩和检验的分组方案的分组类别必须等于2")
                if self.option("mann_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('wilcox秩和检验的置信区间的置信度不在范围值内')
                otu_sample = self.option('mann_input').get_sample_info()
                self.logger.info(otu_sample)
                group_sample, header, is_empty = self.option('mann_group').get_file_info()
                self.check_sample(group_sample, otu_sample)
                self.check_group(self.option('mann_group').prop['path'])
            elif i == "student":
                if not self.option("student_input").is_set:
                    raise OptionError('必须设置student_T检验输入的otutable文件')
                if not self.option("student_group").is_set:
                    raise OptionError('必须设置student_T检验输入的分组文件')
                if self.option("student_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                             "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option("student_ci") <= 0 or self.option("student_ci") >= 1:
                    raise OptionError('所输入的显著水平不在范围值内')
                if self.option("student_type") not in ["two.side", "greater", "less"]:
                    raise OptionError('所输入的类型不在范围值内')
                if not self.option("student_gname"):
                    raise OptionError("student_T检验分组方案选择参数为必须参数，请设置")
                if len(self.option("student_gname").split(',')) != 1:
                    raise OptionError("组间差异的分组方案只能为1个")
                gnum = self.option('student_group').group_num(self.option('student_gname'))
                if gnum != 2:
                    raise OptionError("student_T检验的分组方案的分组类别必须等于2")
                if self.option("student_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('student_T检验的置信区间的置信度不在范围值内')
                otu_sample = self.option('student_input').get_sample_info()
                self.logger.info(otu_sample)
                group_sample, header, is_empty = self.option('student_group').get_file_info()
                self.check_sample(group_sample, otu_sample)
                self.check_group(self.option('student_group').prop['path'])
            elif i == "welch":
                if not self.option("welch_input").is_set:
                    raise OptionError('必须设置welch_T检验输入的otutable文件')
                if not self.option("welch_group").is_set:
                    raise OptionError('必须设置welch_T检验输入的分组文件')
                if self.option("welch_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                           "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option("welch_ci") <= 0 or self.option("welch_ci") >= 1:
                    raise OptionError('所输入的显著水平不在范围值内')
                if self.option("welch_type") not in ["two.side", "greater", "less"]:
                    raise OptionError('所输入的类型不在范围值内')
                if not self.option("welch_gname"):
                    raise OptionError("welch_T检验分组方案选择参数为必须参数，请设置")
                if len(self.option("welch_gname").split(',')) != 1:
                    raise OptionError("组间差异的分组方案只能为1个")
                gnum = self.option('welch_group').group_num(self.option('welch_gname'))
                if gnum != 2:
                    raise OptionError("welch_T检验的分组方案的分组类别必须等于2")
                if self.option("welch_coverage") not in [0.90, 0.95, 0.98, 0.99, 0.999]:
                    raise OptionError('welch_T检验的置信区间的置信度不在范围值内')
                otu_sample = self.option('welch_input').get_sample_info()
                self.logger.info(otu_sample)
                group_sample, header, is_empty = self.option('welch_group').get_file_info()
                self.check_sample(group_sample, otu_sample)
                self.check_group(self.option('welch_group').prop['path'])
            elif i == "estimator":
                if not self.option("est_input").is_set:
                    raise OptionError('必须设置est_T检验输入的多样性指数文件')
                if not self.option("est_group").is_set:
                    raise OptionError('必须设置est_T检验输入的分组文件夹')
                # otu_sample = self.option('est_input').get_sample_info()
                # self.logger.info(otu_sample)
                # group_sample = self.option('est_group').get_file_info()
                # self.check_sample(group_sample, otu_sample)
        return True

    def set_resource(self):
        """
        设置所需资源
        :return:
        """
        self._cpu = 2
        self._memory = '4G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*_result\.xls", "xls", "物种组间差异显著性比较结果表，包括均值，标准差，p值"],
            [r".*_CI\.xls", "xls", "组间差异显著性比较两组，两样本比较的置信区间值以及效果量"],
            [r".*(-).*", "xls", "组间差异显著性比较多组比较的posthoc检验比较的结果，包含置信区间，效果量，p值"],
            [r".*_boxfile\.xls", "xls", "组间差异显著性比较用于画箱线图的数据，包含四分位值"],
            [r"^est_result", "xls", "多样性指数T检验结果分析表，包括均值，标准差，p值，q值"]
            ])
        super(MetastatAgent, self).end()


class MetastatTool(Tool):
    def __init__(self, config):
        super(MetastatTool, self).__init__(config)
        self._version = "v1.0.1"
        self.package_path = 'packages/metastat.py'
        self.r_path = '/program/R-3.3.1/bin/Rscript'

    def run(self):
        """
        运行
        :return:
        """
        super(MetastatTool, self).run()
        self.run_test()
        self.set_output()
        self.end()

    def run_test(self):
        """
        运行metastat.py
        :return:
        """
        for test in self.option('test').split(','):
            self.logger.info(t)
            if test == "chi":
                self.run_chi()
            elif test == "fisher":
                self.run_fisher()
            elif test == "student":
                self.run_student()
            elif test == "welch":
                self.run_welch()
            elif test == "mann":
                self.run_mann()
            elif test == "kru_H":
                self.run_kru()
            elif test == "anova":
                self.run_anova()
            elif test == "estimator":
                self.run_est()

    def run_est(self):    # modify by qindanhua 20170105 add option test method
        gfilelist = os.listdir(self.option("est_group").prop['path'])
        self.logger.info(gfilelist)
        i = 1
        for group in gfilelist:
            est_ttest(self.option('est_input').prop['path'], self.work_dir + '/est_result%s.xls' % i,
                      os.path.join(self.option("est_group").prop['path'], group), self.option("est_test_method"))
            cmd = self.r_path + " run_est_ttest.r"
            self.logger.info("开始运行est_T检验")
            command = self.add_command("est_cmd{}".format(i), cmd).run()
            i += 1
            self.wait(command)
            if command.return_code == 0 or command.return_code is None:
                self.logger.info("est_ttest运行完成")
            else:
                self.set_error("est_ttest运行出错!")

    def run_chi(self):
        two_sample_test(self.option('chi_input').prop['path'], self.work_dir + '/chi_result.xls', "chi",
                        self.option('chi_sample1'), self.option('chi_sample2'), self.option('chi_correction'))
        cmd = self.r_path + " run_chi_test.r"
        self.logger.info("开始运行卡方检验")
        command = self.add_command("chi_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("chi_cmd运行完成，开始运行计算置信区间")
            self.twosample_ci(self.option("chi_methor"), self.option("chi_input").prop['path'],
                              self.work_dir + '/chi_result.xls', self.option('chi_sample1'), self.option('chi_sample2'),
                              self.option('chi_coverage'), self.work_dir + '/chi_CI.xls')
        else:
            self.set_error("chi_cmd运行出错!")
        self.logger.info("chi_test运行完成")

    def run_fisher(self):
        two_sample_test(self.option('fisher_input').prop['path'], self.work_dir + '/fisher_result.xls', "fisher",
                        self.option('fisher_sample1'), self.option('fisher_sample2'),
                        str(1 - self.option('fisher_ci')), self.option('fisher_type'),
                        self.option('fisher_correction'))
        cmd = self.r_path + " run_fisher_test.r"
        self.logger.info("开始运行fisher检验")
        command = self.add_command("fisher_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("fisher_cmd运行完成，开始运行计算置信区间")
            self.twosample_ci(self.option("fisher_methor"), self.option("fisher_input").prop['path'],
                              self.work_dir + '/fisher_result.xls', self.option('fisher_sample1'),
                              self.option('fisher_sample2'), self.option('fisher_coverage'),
                              self.work_dir + '/fisher_CI.xls')
        else:
            self.set_error("fisher_cmd运行出错!")
        self.logger.info("fisher_test运行完成")

    def twosample_ci(self, methor, otufile, statfile, sample1, sample2, coverage, outfile):
        if methor == "DiffBetweenPropAsymptoticCC":
            DiffBetweenPropAsymptoticCC(otufile, statfile, sample1, sample2, coverage, outfile)
        if methor == "DiffBetweenPropAsymptotic":
            DiffBetweenPropAsymptotic(otufile, statfile, sample1, sample2, coverage, outfile)
        if methor == "NewcombeWilson":
            NewcombeWilson(otufile, statfile, sample1, sample2, coverage, outfile)

    def run_student(self):
        glist = [self.option('student_gname')]
        self.option('student_group').sub_group('./student_group', glist)
        two_group_test(self.option('student_input').prop['path'], './student_group',
                       self.work_dir + '/student_result.xls', self.work_dir + '/student_boxfile.xls', "student",
                       str(1 - self.option('student_ci')), self.option('student_type'),
                       self.option('student_correction'))
        cmd = self.r_path + " run_student_test.r"
        self.logger.info("开始运行student_T检验")
        command = self.add_command("student_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("student_cmd运行完成，开始运行计算置信区间")
            student(self.work_dir + '/student_result.xls', './student_group', self.option('student_coverage'))
            self.logger.info("生成单物种柱状图的数据")
            group_bar(self.option('student_input').prop['path'], './student_group', self.work_dir + '/student_plot_group_bar.xls', "student")
            cmd1 = self.r_path + " run_student_bar.r"
            bar_cmd = self.add_command("bar_cmd", cmd1).run()
            self.wait(bar_cmd)
            if bar_cmd.return_code == 0:
                self.logger.info("student_test运行完成")
            else:
                self.set_error("bar_cmd运行出错!")
        else:
            self.set_error("student_cmd运行出错!")

    def run_welch(self):
        glist = [self.option('welch_gname')]
        self.option('welch_group').sub_group('./welch_group', glist)
        two_group_test(self.option('welch_input').prop['path'], './welch_group',
                       self.work_dir + '/welch_result.xls', self.work_dir + '/welch_boxfile.xls', "welch",
                       str(1 - self.option('welch_ci')), self.option('welch_type'),
                       self.option('welch_correction'))
        self.logger.info(Config().SOFTWARE_DIR)
        cmd = self.r_path + " run_welch_test.r"
        self.logger.info("开始运行welch_T检验")
        command = self.add_command("welch_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("welch_cmd运行完成，开始运行计算置信区间")
            welch(self.work_dir + '/welch_result.xls', './welch_group', self.option('welch_coverage'))
            self.logger.info("生成单物种柱状图的数据")
            group_bar(self.option('welch_input').prop['path'], './welch_group', self.work_dir + '/welch_plot_group_bar.xls', 'welch')
            cmd1 = self.r_path + " run_welch_bar.r"
            bar_cmd = self.add_command("bar_cmd", cmd1).run()
            self.wait(bar_cmd)
            if bar_cmd.return_code == 0:
                self.logger.info("welch_cmd运行完成")
            else:
                self.set_error("bar_cmd运行出错!")
        else:
            self.set_error("welch_cmd运行出错!")

    def run_mann(self):
        glist = [self.option('mann_gname')]
        self.option('mann_group').sub_group('./mann_group', glist)
        two_group_test(self.option('mann_input').prop['path'], './mann_group',
                       self.work_dir + '/mann_result.xls', self.work_dir + '/mann_boxfile.xls', "mann",
                       str(1 - self.option('mann_ci')), self.option('mann_type'),
                       self.option('mann_correction'))
        cmd = self.r_path + " run_mann_test.r"
        self.logger.info("开始运行mann检验")
        command = self.add_command("mann_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("mann_cmd运行完成，开始运行计算置信区间")
            student(self.work_dir + '/mann_result.xls', './mann_group', self.option('mann_coverage'))
            self.logger.info("生成单物种柱状图的数据")
            group_bar(self.option('mann_input').prop['path'], './mann_group', self.work_dir + '/mann_plot_group_bar.xls', 'mann')
            cmd1 = self.r_path + " run_mann_bar.r"
            bar_cmd = self.add_command("bar_cmd", cmd1).run()
            self.wait(bar_cmd)
            if bar_cmd.return_code == 0:
                self.logger.info("mann_test运行完成")
            else:
                self.set_error("bar_cmd运行出错!")

        else:
            self.set_error("mann_cmd运行出错!")

    def run_kru(self):
        glist = [self.option('kru_H_gname')]
        self.option('kru_H_group').sub_group('./kru_H_group', glist)
        mul_group_test(self.option('kru_H_input').prop['path'], self.work_dir + '/kru_H_result.xls',
                       self.work_dir + '/kru_H_boxfile.xls', './kru_H_group', "kru_H",
                       self.option('kru_H_correction'))
        cmd = self.r_path + " run_kru_H_test.r"
        self.logger.info("开始运行kru_H检验")
        command = self.add_command("kru_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("kru_cmd运行完成，开始运行post-hoc检验")
            self.posthoc(self.option("kru_H_methor"), self.work_dir + '/kru_H_result.xls', './kru_H_group',
                         self.option("kru_H_coverage"), './kru_H')
            self.logger.info("生成单物种柱状图的数据")
            group_bar(self.option('kru_H_input').prop['path'], './kru_H_group', self.work_dir + '/kru_H_plot_group_bar.xls', 'kru_H')
            cmd1 = self.r_path + " run_kru_H_bar.r"
            bar_cmd = self.add_command("bar_cmd", cmd1).run()
            self.wait(bar_cmd)
            if bar_cmd.return_code == 0:
                self.logger.info("kru_H_test运行完成")
            else:
                self.set_error("bar_cmd运行出错!")
        else:
            self.set_error("kru_cmd运行出错!")

    def run_anova(self):
        glist = [self.option('anova_gname')]
        self.option('anova_group').sub_group('./anova_group', glist)
        mul_group_test(self.option('anova_input').prop['path'], self.work_dir + '/anova_result.xls',
                       self.work_dir + '/anova_boxfile.xls', './anova_group', "anova",
                       self.option('anova_correction'))
        cmd = self.r_path + " run_anova_test.r"
        self.logger.info("开始运行anova检验")
        command = self.add_command("anova_cmd", cmd).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("anova_cmd运行完成,开始运行post-hoc检验")
            self.posthoc(self.option("anova_methor"), self.work_dir + '/anova_result.xls',
                         './anova_group', self.option("anova_coverage"), './anova')
            self.logger.info("生成单物种柱状图的数据")
            group_bar(self.option('anova_input').prop['path'], './anova_group', self.work_dir + '/anova_plot_group_bar.xls', 'anova')
            cmd1 = self.r_path + " run_anova_bar.r"
            bar_cmd = self.add_command("bar_cmd", cmd1).run()
            self.wait(bar_cmd)
            if bar_cmd.return_code == 0:
                self.logger.info("anova_test运行完成")
            else:
                self.set_error("bar_cmd运行出错!")
        else:
            self.set_error("anova_cmd运行出错!")

    def posthoc(self, methor, statfile, groupfile, coverage, outfile):
        if methor == 'tukeykramer':
            tukeykramer(statfile, groupfile, coverage, outfile)
        if methor == 'gameshowell':
            gameshowell(statfile, groupfile, coverage, outfile)
        if methor == 'welchuncorrected':
            welchuncorrected(statfile, groupfile, coverage, outfile)
        if methor == 'scheffe':
            scheffe(statfile, groupfile, coverage, outfile)

    def set_output(self):
        """
        将结果文件link到output文件夹下面
        :return:
        """
        for root, dirs, files in os.walk(self.output_dir):
            for names in files:
                os.remove(os.path.join(root, names))
        self.logger.info("设置结果目录")
        for t in self.option('test').split(','):
            if t == 'chi':
                try:
                    os.link(self.work_dir + '/chi_result.xls', self.output_dir + '/chi_result.xls')
                    os.link(self.work_dir + '/chi_CI.xls', self.output_dir + '/chi_CI.xls')
                    self.logger.info("设置chi分析的结果目录成功")
                except:
                    self.logger.info("设置chi分析结果目录失败")
            elif t == 'fisher':
                try:
                    os.link(self.work_dir + '/fisher_result.xls', self.output_dir + '/fisher_result.xls')
                    os.link(self.work_dir + '/fisher_CI.xls', self.output_dir + '/fisher_CI.xls')
                    self.logger.info("设置fisher分析的结果目录成功")
                except:
                    self.logger.info("设置fisher分析结果目录失败")
            elif t == 'student':
                try:
                    os.link(self.work_dir + '/student_result.xls', self.output_dir + '/student_result.xls')
                    os.link(self.work_dir + '/student_boxfile.xls', self.output_dir + '/student_boxfile.xls')
                    os.link(self.work_dir + '/student_CI.xls', self.output_dir + '/student_CI.xls')
                    self.logger.info("设置student分析的结果目录成功")
                except:
                    self.logger.info("设置student分析结果目录失败")
            elif t == 'welch':
                try:
                    os.link(self.work_dir + '/welch_result.xls', self.output_dir + '/welch_result.xls')
                    os.link(self.work_dir + '/welch_boxfile.xls', self.output_dir + '/welch_boxfile.xls')
                    os.link(self.work_dir + '/welch_CI.xls', self.output_dir + '/welch_CI.xls')
                    self.logger.info("设置welch分析的结果目录成功")
                except:
                    self.logger.info("设置welch分析结果目录失败")
            elif t == 'mann':
                try:
                    os.link(self.work_dir + '/mann_result.xls', self.output_dir + '/mann_result.xls')
                    os.link(self.work_dir + '/mann_boxfile.xls', self.output_dir + '/mann_boxfile.xls')
                    os.link(self.work_dir + '/student_CI.xls', self.output_dir + '/mann_CI.xls')
                    self.logger.info("设置mann分析的结果目录成功")
                except:
                    self.logger.info("设置mann分析结果目录失败")
            elif t == 'anova':
                try:
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/anova_result.xls'))
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/anova_boxfile.xls'))
                    os.link(self.work_dir + '/anova_result.xls', self.output_dir + '/anova_result.xls')
                    os.link(self.work_dir + '/anova_boxfile.xls', self.output_dir + '/anova_boxfile.xls')
                    for r, d, f in os.walk(self.work_dir, topdown=False):
                        filelist = f
                        self.logger.info(filelist)
                    for i in filelist:
                        if re.match(r'^anova_%s' % self.option("anova_methor"), i):
                            os.link(self.work_dir + '/' + i, self.output_dir + '/' + i)
                    self.logger.info("设置anova分析的结果目录成功")
                except:
                    self.logger.info("设置anova分析结果目录失败")
            elif t == 'kru_H':
                try:
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/kru_H_result.xls'))
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/kru_H_boxfile.xls'))
                    os.link(self.work_dir + '/kru_H_result.xls', self.output_dir + '/kru_H_result.xls')
                    os.link(self.work_dir + '/kru_H_boxfile.xls', self.output_dir + '/kru_H_boxfile.xls')
                    for r, d, f in os.walk(self.work_dir, topdown=False):
                        filelist = f
                    for i in filelist:
                        if re.match(r'^kru_H_%s' % self.option("kru_H_methor"), i):
                            os.link(self.work_dir + '/' + i, self.output_dir + '/' + i)
                    self.logger.info("设置kru_H分析的结果目录成功")
                except:
                    self.logger.info("设置kru_H分析结果目录失败")
            elif t == 'estimator':
                try:
                    for r, d, f in os.walk(self.work_dir, topdown=False):
                        filelist = f
                    for i in filelist:
                        if re.match(r'^est_result', i):
                            os.link(self.work_dir + '/' + i, self.output_dir + '/' + i)
                    self.logger.info("设置est分析的结果目录成功")
                except:
                    self.logger.info("设置est分析结果目录失败")
