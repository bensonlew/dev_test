# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""组间差异性多组比较检验分析"""

from biocluster.workflow import Workflow
import os


class MultipleWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(MultipleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            # {"name": "type", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "test", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "correction", "type": "string", "default": "none"},
            {"name": "params", "type": "string"},
            {"name": "group_name", "type": "string"},
            {"name": "methor", "type": "string"},
            {"name": "coverage", "type": "float"}

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.multiple = self.add_tool("statistical.metastat")
        self.output_dir = self.multiple.output_dir

    def run_multiple(self):
        if self.option("test") == "anova":
            options = {
                "anova_input": self.option("otu_file"),
                "anova_group": self.option("group_file"),
                "anova_correction": self.option("correction"),
                "test": self.option("test"),
                "anova_gname": self.option("group_name"),
                "anova_methor": self.option("methor"),
                "anova_coverage": self.option("coverage")
            }
        else:
            options = {
                "kru_H_input": self.option("otu_file"),
                "kru_H_group": self.option("group_file"),
                "kru_H_correction": self.option("correction"),
                # "kru_H_type": self.option("type"),
                "test": self.option("test"),
                "kru_H_gname": self.option("group_name"),
                "kru_H_methor": self.option("methor"),
                "kru_H_coverage": self.option("coverage")
            }
        self.multiple.set_options(options)
        self.multiple.on("end", self.set_db)
        self.multiple.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*_result\.xls", "xls", "物种组间差异显著性比较结果表，包括均值，标准差，p值"],
            [r".*_CI\.xls", "xls", "组间差异显著性比较两组，两样本比较的置信区间值以及效果量"],
            [r".*(-).*", "xls", "组间差异显著性比较多组比较的posthoc检验比较的结果，包含置信区间，效果量，p值"],
            [r".*_boxfile\.xls", "xls", "组间差异显著性比较用于画箱线图的数据，包含四分位值"]
            ])
        super(MultipleWorkflow, self).end()

    def set_db(self):
        """
        保存两组比较分析的结果表保存到mongo数据库中
        """
        api_multiple = self.api.stat_test
        stat_path = self.output_dir + '/' + self.option("test") + '_result.xls'
        boxfile_path = self.output_dir + '/' + self.option("test") + '_boxfile.xls'
        params = eval(self.option("params"))
        main_id = api_multiple.add_species_difference_check_detail(file_path=stat_path, table_id=None, level=self.option("level"), check_type='multiple', params=self.option("params"), group_id=params["group_id"], from_otu_table=params["otu_id"], major=True)
        if not os.path.isfile(stat_path):
            raise Exception("找不到报告文件:{}".format(stat_path))
        if not os.path.isfile(boxfile_path):
            raise Exception("找不到报告文件:{}".format(boxfile_path))
        for r, d, f in os.walk(self.output_dir):
            for i in f:
                if self.option("methor") in i:
                    ci_path = r + '/' + i
                    if not os.path.isfile(ci_path):
                        raise Exception("找不到报告文件:{}".format(ci_path))
                    api_multiple.add_mulgroup_species_difference_check_ci_plot(file_path=ci_path,
                                                                               table_id=main_id,
                                                                               methor=self.option("methor"))
        api_multiple.add_species_difference_check_boxplot(boxfile_path, main_id)
        self.add_return_mongo_id('sg_species_difference_check', main_id)
        self.end()

    def run(self):
        self.run_multiple()
        super(MultipleWorkflow, self).run()
