# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""组间差异两样品比较检验分析"""

from biocluster.workflow import Workflow
import os


class TwoSampleWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(TwoSampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "test", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "two_sample_id", "type": "string"},
            {"name": "correction", "type": "string"},
            {"name": "ci", "type": "float"},
            {"name": "sample1", "type": "string"},
            {"name": "sample2", "type": "string"}

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
                "test": self.option("test")


            }
        else:
            options = {
                "fisher_input": self.option("otu_file"),
                "fisher_ci": self.option("ci"),
                "fisher_sample1": self.option("sample1"),
                "fisher_sample2": self.option("sample2"),
                "fisher_correction": self.option("correction"),
                "test": self.option("test"),
                "fisher_type": self.option("type")
            }
        self.two_sample.set_options(options)
        self.on_rely(self.two_sample, self.set_db)
        self.two_sample.run()

    def set_db(self):
        api_two_sample = self.api.stat_test
        two_sample_path = self.output_dir + '/' + self.option("test") + '_result.xls'
        if not os.path.isfile(two_sample_path):
            raise Exception("找不到报告文件:{}".format(two_sample_path))
        api_two_sample.add_species_difference_check_detail(two_sample_path, self.option("two_sample_id"))
        
        self.end()

    def run(self):
        self.run_two_sample()
        super(TwoSampleWorkflow, self).run()
