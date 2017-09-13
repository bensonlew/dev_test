# -*- coding: utf-8 -*-
# __author__ = zengjing
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import pandas as pd


class CircosAgent(Agent):
    """
    小工具弦图：对二维表格(行为样本)进行合并
    """
    def __init__(self, parent):
        super(CircosAgent, self).__init__(parent)
        options = [
            {"name": "data_table", "type": "infile", "format": "toolapps.table"},
            {"name": "group_table", "type": "infile", "format": "toolapps.group_table"},
            {"name": "merge_value", "type": "float"}  #合并小于此数值的区域的值
        ]
        self.add_option(options)
        self.step.add_steps("circos")

    def check_options(self):
        if not self.option("data_table").is_set:
            raise OptionError("缺少输入的数据表格")

    def set_resource(self):
        self._cpu = 10
        self._memory = "10G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "Circos结果目录"]
        ])


class CircosTool(Tool):
    def __init__(self, config):
        super(CircosTool, self).__init__(config)
        self._version = 1.0

    def data_table_stat(self):
        fp = self.option("data_table").prop["path"]
        data = pd.read_table(fp, header=0)
        self.
        with open(fp, "r") as f, open("new_data.xls", "w") as w:
            lines = f.readlines()
            header = lines[0]
            w.write("#name\tall\n")
            for line in lines[1:]:
                item = line.strip().split("\t")
                new_line = []
                w.write(item[0] + "\t")
                sum = 0
                for i in range(1, len(item)):
                    sum += item[i]
