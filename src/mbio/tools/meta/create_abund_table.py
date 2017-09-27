# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modifiy:2017.09.27

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import pandas as pd


class CreateAbundTableAgent(Agent):
    """
    生成各项分析的不同数据库的标准丰度表格
    """

    def __init__(self, parent):
        super(CreateAbundTableAgent, self).__init__(parent)
        options = [
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "level_type", "type": "string", "default": ""}  # 注释表的字段，eg：ARG， Type， Class
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("anno_table").is_set:
            raise OptionError("请传入注释表格！")
        if not self.option("geneset_table").is_set:
            raise OptionError("请传入基因丰度文件！")
        if self.option("level_type") == "":
            raise OptionError("请提供筛选的level水平")

    def set_resource(self):
        self._cpu = 1
        self._memory = '2G'


class CreateAbundTableTool(Tool):
    def __init__(self, config):
        super(CreateAbundTableTool, self).__init__(config)

    def create_abund_table(self):
        anno_table_path = self.option("anno_table").prop["path"]
        geneset_table_path = self.option("geneset_table").prop["path"]
        anno_table = pd.read_table(anno_table_path, sep='\t', header=0)
        a = pd.DataFrame(anno_table)
        a = a.ix[:, ["#Query", "Type"]]
        a.columns = ["GeneID", "Type"]
        geneset_table = pd.read_table(geneset_table_path, sep='\t', header=0)
        b = pd.DataFrame(geneset_table)
        abund = a.merge(b, on='GeneID', how='inner')
        abund_table = abund.groupby(self.option("level_type")).sum()
        new_otu_file_path = os.path.join(self.output_dir, "new_abund_table.xls")
        abund_table.to_csv(new_otu_file_path, sep="\t")

    def run(self):
        super(CreateAbundTableTool, self).run()
        self.create_abund_table()
        self.end()
