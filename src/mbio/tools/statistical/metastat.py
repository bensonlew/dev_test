# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.statistical.metastat import *
import subprocess
import os


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
            {"name": "kru_H_group", "type": "infile", "format": "meta.otu.group_table"},  # kruskal_wallis_H_test的输入分组文件
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
            {"name": "student_type", "type": "string", "default":"two.side"},  # T检验的选择单尾或双尾检验
            {"name": "welch_input", "type": "infile", "format": "meta.otu.otu_table"},  # welch_T检验的输入文件
            {"name": "welch_ci", "type": "float", "default": 0.05},  # welch_T检验的显著性水平
            {"name": "welch_group", "type": "infile", "format": "meta.otu.group_table"},  # welch_T检验的输入分组文件
            {"name": "welch_correction", "type": "string", "default": "none"},  # welch_T检验的多重检验校正
            {"name": "welch_type", "type": "string", "default": "two.side"},  # welch_T检验的选择单尾或双尾检验
            {"name": "anova_input", "type": "infile", "format": "meta.otu.otu_table"},  # anova分析的输入文件
            {"name": "anova_group", "type": "infile", "format": "meta.otu.group_table"},  # anova分析的输入分组文件
            {"name": "anova_correction", "type": "string", "default": "none"},  # anova分析的多重检验校正
            {"name": "test", "type": "string"},   #选择统计学检验分析方法
            {"name": "student_gname", "type": "string", "default": 'None'}, #student检验分组方案选择
            {"name": "welch_gname", "type": "string", "default": 'None'}, #welch检验分组方案选择
            {"name": "mann_gname", "type": "string", "default": 'None'},  #wilcox秩和检验分组方案选择
            {"name": "kru_H_gname", "type": "string", "default": 'None'}, #kru检验分组方案选择
            {"name": "anova_gname", "type": "string", "default": 'None'} #单因素方差分析分组方案选择
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

    def check_options(self):
        """
        检查参数设置
        :return:
        """
        if not self.option('test'):
            self.logger.info(self.option('test'))
            raise OptionError("必须设置输入的检验名称")
        for i in self.option('test').split(','):
            if i not in ["chi", "fisher", "kru_H", "mann", "anova", "student", "welch"]:
                raise OptionError("所输入的检验名称不对")
            elif i == "chi":
                if not self.option("chi_input").is_set:
                    raise OptionError('必须设置卡方检验输入的otutable文件')
                if not self.option("chi_sample1") and not self.option("chi_sample2"):
                    raise OptionError('必须设置卡方检验要比较的样品名')
                if self.option("chi_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr",
                                                         "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
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
            elif i == "kru_H":
                if not self.option("kru_H_input").is_set:
                    raise OptionError('必须设置kruskal_wallis_H_test输入的otutable文件')
                if not self.option("kru_H_group").is_set:
                    raise OptionError('必须设置kruskal_wallis_H_test输入的分组文件')
                if self.option("kru_H_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                           "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option('kru_H_gname') != 'None':
                    gnum = self.option('kru_H_group').group_num(self.option('kru_H_gname'))
                    if gnum < 3:
                        raise OptionError("kru检验的分组方案的分组类别必须大于等于3")
                # if self.option("kru_H_type") not in ["two.side", "greater", "less"]:
                #     raise OptionError('所输入的类型不在范围值内')
            elif i == "anova":
                if not self.option("anova_input").is_set:
                    raise OptionError('必须设置kruskal_wallis_H_test输入的otutable文件')
                if not self.option("anova_group").is_set:
                    raise OptionError('必须设置kruskal_wallis_H_test输入的分组文件')
                if self.option("anova_correction") not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY",
                                                           "fdr", "none"]:
                    raise OptionError('该多重检验校正的方法不被支持')
                if self.option('anova_gname') != 'None':
                    gnum = self.option('anova_group').group_num(self.option('anova_gname'))
                    if gnum < 3:
                        raise OptionError("anova检验的分组方案的分组类别必须大于等于3")
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
                if self.option('mann_gname') != 'None':
                    gnum = self.option('mann_group').group_num(self.option('mann_gname'))
                    if gnum != 2:
                        raise OptionError("mann检验的分组方案的分组类别必须等于2")
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
                if self.option('student_gname') != 'None':
                    gnum = self.option('student_group').group_num(self.option('student_gname'))
                    if gnum != 2:
                        raise OptionError("student检验的分组方案的分组类别必须等于2")
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
                if self.option('welch_gname') != 'None':
                    gnum = self.option('welch_group').group_num(self.option('welch_gname'))
                    if gnum != 2:
                        raise OptionError("mann检验的分组方案的分组类别必须等于2")
        return True

    def set_resource(self):
        """
        设置所需资源
        :return:
        """
        self._cpu = 10
        self._memory = ''


class MetastatTool(Tool):
    def __init__(self, config):
        super(MetastatTool, self).__init__(config)
        self._version = "v1.0.1"
        self.package_path = 'packages/metastat.py'

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

        def stats_update(self):
            self.step.stat_test.start()
            self.step.stat_test.finish()
            self.step.update()

        for t in self.option('test').split(','):
            self.logger.info(t)
            if t == "chi":
                two_sample_test(self.option('chi_input').prop['path'], self.work_dir + '/chi_result.xls', t,
                                self.option('chi_sample1'), self.option('chi_sample2'), self.option('chi_correction'))
                cmd = "%s/R-3.2.2/bin/Rscript run_chi_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("chi_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("chi_test运行出错")
            elif t == "fisher":
                two_sample_test(self.option('fisher_input').prop['path'], self.work_dir + '/fisher_result.xls', t,
                                self.option('fisher_sample1'), self.option('fisher_sample2'),
                                str(1 - self.option('fisher_ci')), self.option('fisher_type'),
                                self.option('fisher_correction'))
                cmd = "%s/R-3.2.2/bin/Rscript run_fisher_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("fisher_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("fisher_test运行出错")
            elif t == "student":
                if self.option('student_gname') == 'None':
                    two_group_test(self.option('student_input').prop['path'], self.option('student_group').prop['path'],
                                   self.work_dir + '/student_result.xls', self.work_dir + '/student_boxfile.xls', t,
                                   str(1 - self.option('student_ci')), self.option('student_type'),
                                   self.option('student_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_student_test.r" % Config().SOFTWARE_DIR
                else:
                    glist = [self.option('student_gname')]
                    self.option('student_group').sub_group('./student_group', glist)
                    two_group_test(self.option('student_input').prop['path'], './student_group',
                                   self.work_dir + '/student_result.xls', self.work_dir + '/student_boxfile.xls', t,
                                   str(1 - self.option('student_ci')), self.option('student_type'),
                                   self.option('student_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_student_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("student_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("student_test运行出错")
            elif t == "welch":
                if self.option('welch_gname') == 'None':
                    two_group_test(self.option('welch_input').prop['path'], self.option('welch_group').prop['path'],
                                   self.work_dir + '/welch_result.xls', self.work_dir + '/welch_boxfile.xls', t,
                                   str(1 - self.option('welch_ci')),
                                   self.option('welch_type'), self.option('welch_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_welch_test.r" % Config().SOFTWARE_DIR
                else:
                    glist = [self.option('welch_gname')]
                    self.option('welch_group').sub_group('./welch_group', glist)
                    two_group_test(self.option('welch_input').prop['path'], './welch_group',
                                   self.work_dir + '/welch_result.xls', self.work_dir + '/welch_boxfile.xls', t,
                                   str(1 - self.option('welch_ci')), self.option('welch_type'),
                                   self.option('welch_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_welch_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("welch_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("welch_test运行出错")
            elif t == "mann":
                if self.option('mann_gname') == 'None':
                    two_group_test(self.option('mann_input').prop['path'], self.option('mann_group').prop['path'],
                                   self.work_dir + '/mann_result.xls', self.work_dir + '/mann_boxfile.xls', t,
                                   str(1 - self.option('mann_ci')), self.option('mann_type'),
                                   self.option('mann_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_mann_test.r" % Config().SOFTWARE_DIR
                else:
                    glist = [self.option('mann_gname')]
                    self.option('mann_group').sub_group('./mann_group', glist)
                    two_group_test(self.option('mann_input').prop['path'], './mann_group',
                                   self.work_dir + '/mann_result.xls', self.work_dir + '/mann_boxfile.xls', t,
                                   str(1 - self.option('mann_ci')), self.option('mann_type'),
                                   self.option('mann_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_mann_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("mann_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("mann_test运行出错")
            elif t == "kru_H":
                if self.option('kru_H_gname') == 'None':
                    mul_group_test(self.option('kru_H_input').prop['path'], self.work_dir + '/kru_H_result.xls',
                                   self.work_dir + '/kru_H_boxfile.xls', self.option('kru_H_group').prop['path'], t,
                                   self.option('kru_H_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_kru_H_test.r" % Config().SOFTWARE_DIR
                else:
                    glist = [self.option('kru_H_gname')]
                    self.option('kru_H_group').sub_group('./kru_H_group', glist)
                    mul_group_test(self.option('kru_H_input').prop['path'], self.work_dir + '/kru_H_result.xls',
                                   self.work_dir + '/kru_H_boxfile.xls', './kru_H_group', t,
                                   self.option('kru_H_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_kru_H_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("kru_H_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("kru_H_test运行出错")
            elif t == "anova":
                if self.option('anova_gname') == 'None':
                    mul_group_test(self.option('anova_input').prop['path'], self.work_dir + '/anova_result.xls',
                                   self.work_dir + '/anova_boxfile.xls', self.option('anova_group').prop['path'], t,
                                   self.option('anova_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_anova_test.r" % Config().SOFTWARE_DIR
                else:
                    glist = [self.option('anova_gname')]
                    self.option('anova_group').sub_group('./anova_group', glist)
                    mul_group_test(self.option('anova_input').prop['path'], self.work_dir + '/anova_result.xls',
                                   self.work_dir + '/anova_boxfile.xls', './anova_group', t,
                                   self.option('anova_correction'))
                    cmd = "%s/R-3.2.2/bin/Rscript run_anova_test.r" % Config().SOFTWARE_DIR
                try:
                    subprocess.check_output(cmd, shell=True)
                    self.logger.info("anova_test运行完成")
                except subprocess.CalledProcessError:
                    self.logger.info("anova_test运行出错")


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
                    self.logger.info("设置chi分析的结果目录成功")
                except:
                    self.logger.info("设置chi分析结果目录失败")
            elif t == 'fisher':
                try:
                    os.link(self.work_dir + '/fisher_result.xls', self.output_dir + '/fisher_result.xls')
                    self.logger.info("设置fisher分析的结果目录成功")
                except:
                    self.logger.info("设置fisher分析结果目录失败")
            elif t == 'student':
                try:
                    os.link(self.work_dir + '/student_result.xls', self.output_dir + '/student_result.xls')
                    os.link(self.work_dir + '/student_boxfile.xls', self.output_dir + '/student_boxfile.xls')
                    self.logger.info("设置chi分析的结果目录成功")
                except:
                    self.logger.info("设置fisher分析结果目录失败")
            elif t == 'welch':
                try:
                    os.link(self.work_dir + '/welch_result.xls', self.output_dir + '/welch_result.xls')
                    os.link(self.work_dir + '/welch_boxfile.xls', self.output_dir + '/welch_boxfile.xls')
                    self.logger.info("设置welch分析的结果目录成功")
                except:
                    self.logger.info("设置welch分析结果目录失败")
            elif t == 'mann':
                try:
                    os.link(self.work_dir + '/mann_result.xls', self.output_dir + '/mann_result.xls')
                    os.link(self.work_dir + '/mann_boxfile.xls', self.output_dir + '/mann_boxfile.xls')
                    self.logger.info("设置mann分析的结果目录成功")
                except:
                    self.logger.info("设置mann分析结果目录失败")
            elif t == 'anova':
                try:
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/anova_result.xls'))
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/anova_boxfile.xls'))
                    os.link(self.work_dir + '/anova_result.xls', self.output_dir + '/anova_result.xls')
                    os.link(self.work_dir + '/anova_boxfile.xls', self.output_dir + '/anova_boxfile.xls')
                    self.logger.info("设置anova分析的结果目录成功")
                except:
                    self.logger.info("设置anova分析结果目录失败")
            elif t == 'kru_H':
                try:
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/kru_H_result.xls'))
                    # os.system('sed -i "1s/\(^.\)/\t\1/" %s' % (self.work_dir + '/kru_H_boxfile.xls'))
                    os.link(self.work_dir + '/kru_H_result.xls', self.output_dir + '/kru_H_result.xls')
                    os.link(self.work_dir + '/kru_H_boxfile.xls', self.output_dir + '/kru_H_boxfile.xls')
                    self.logger.info("设置kru_H分析的结果目录成功")
                except:
                    self.logger.info("设置kru_H分析结果目录失败")


