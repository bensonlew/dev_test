# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""组间差异性两组比较检验分析"""

from biocluster.workflow import Workflow
import os


class TwoGroupWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(TwoGroupWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string"},
            {"name": "type", "type": "string", "default": "two.side"},
            {"name": "update_info", "type": "string"},
            {"name": "test", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "correction", "type": "string", "default": "none"},
            {"name": "ci", "type": "float", "default": 0.05},
            {"name": "two_group_id", "type": "string"},
            {"name": "group_name", "type": "string"}

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.two_group = self.add_tool("statistical.metastat")

    def run_two_group(self):
        if self.option("test") == "student":
            options = {
                "student_input": self.option("otu_file"),
                "student_group": self.option("group_file"),
                "student_ci": self.option("ci"),
                "student_correction": self.option("correction"),
                "student_type": self.option("type"),
                "test": self.option("test"),
                "student_gname": self.option("group_name")
            }
        elif self.option("test") == "mann":
            options = {
                "mann_input": self.option("otu_file"),
                "mann_ci": self.option("ci"),
                "mann_group": self.option("group_file"),
                "mann_correction": self.option("correction"),
                "mann_type": self.option("type"),
                "test": self.option("test"),
                "mann_gname": self.option("group_name")
            }
        else:
            options = {
                "welch_input": self.option("otu_file"),
                "welch_ci": self.option("ci"),
                "welch_group": self.option("group_file"),
                "welch_correction": self.option("correction"),
                "welch_type": self.option("type"),
                "test": self.option("test"),
                "welch_gname": self.option("group_name")
            }
        self.two_group.set_options(options)
        self.on_rely(self.two_group, self.set_db)
        self.output_dir = self.two_group.output_dir
        self.two_group.run()

    def set_db(self):
        """
        保存两组比较分析的结果表保存到mongo数据库中
        """
        api_two_group = self.api.stat_test
        stat_path = self.output_dir + '/' + self.option("test") + '_result.xls'
        boxfile_path = self.output_dir + '/' + self.option("test") + '_boxfile.xls'
        if not os.path.isfile(stat_path):
            raise Exception("找不到报告文件:{}".format(stat_path))
        if not os.path.isfile(boxfile_path):
            raise Exception("找不到报告文件:{}".format(boxfile_path))
        api_two_group.add_species_difference_check_detail(file_path=stat_path, table_id=self.option("two_group_id"))
        api_two_group.add_species_difference_check_boxplot(boxfile_path, self.option("two_group_id"))
        self.end()

    def run(self):
        self.run_two_group()
        super(TwoGroupWorkflow, self).run()
