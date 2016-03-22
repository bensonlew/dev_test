# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""组间差异两样品比较检验分析"""

from biocluster.workflow import Workflow
from mbio.packages.statistical.twosample_bar import *
import os


class TwoSampleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(TwoSampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "type", "type": "string", "default": "two.side"},
            {"name": "update_info", "type": "string"},
            {"name": "test", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "two_sample_id", "type": "string"},
            {"name": "correction", "type": "string", "default": "none"},
            {"name": "ci", "type": "float", "default": 0.05},
            {"name": "sample1", "type": "string"},
            {"name": "sample2", "type": "string"},
            {"name": "methor", "type": "string"},
            {"name": "coverage", "type": "float"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.two_sample = self.add_tool("statistical.metastat")

    def run_two_sample(self):
        if self.option("test") == "chi":
            options = {
                "chi_input": self.option("otu_file"),
                "chi_sample1": self.option("sample1"),
                "chi_sample2": self.option("sample2"),
                "chi_correction": self.option("correction"),
                "test": self.option("test"),
                "chi_methor": self.option("methor"),
                "chi_coverage": self.option("coverage")
            }
        else:
            options = {
                "fisher_input": self.option("otu_file"),
                "fisher_ci": self.option("ci"),
                "fisher_sample1": self.option("sample1"),
                "fisher_sample2": self.option("sample2"),
                "fisher_correction": self.option("correction"),
                "test": self.option("test"),
                "fisher_type": self.option("type"),
                "fisher_methor": self.option("methor"),
                "fisher_coverage": self.option("coverage")
            }
        self.two_sample.set_options(options)
        self.output_dir = self.two_sample.output_dir
        self.on_rely(self.two_sample, self.set_db)
        self.two_sample.run()

    def set_db(self):
        api_two_sample = self.api.stat_test
        two_sample_path = self.output_dir + '/' + self.option("test") + '_result.xls'
        ci_path = self.output_dir + '/' + self.option("test") + '_CI.xls'
        errorbar_path = self.output_dir + '/' + 'errorbar.png'
        #plot error bar
        extended_error_bar(two_sample_path, self.option("sample1"), self.option("sample2"), ci_path, errorbar_path)
        if not os.path.isfile(two_sample_path):
            raise Exception("找不到报告文件:{}".format(two_sample_path))
        if not os.path.isfile(ci_path):
            raise Exception("找不到报告文件:{}".format(ci_path))
        if not os.path.isfile(errorbar_path):
            raise Exception("找不到报告文件:{}".format(errorbar_path))
        api_two_sample.add_twosample_species_difference_check_detail(file_path=two_sample_path,
                                                                     table_id=self.option("two_sample_id"))
        api_two_sample.add_species_difference_check_ci_plot(file_path=ci_path, table_id=self.option("two_sample_id"))
        api_two_sample.update_species_difference_check(errorbar_path, self.option("two_sample_id"), two_sample_path, ci_path, 'twosample')
        self.end()

    def run(self):
        self.run_two_sample()
        super(TwoSampleWorkflow, self).run()
