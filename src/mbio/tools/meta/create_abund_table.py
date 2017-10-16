# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modifiy:2017.10.09

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
            {"name": "gene_list", "type": "infile", "format": "meta.profile"},
            {"name": "level_type", "type": "string", "default": ""},
            # 注释表的字段，eg：Pathway，Level1，Level2
            {"name": "level_type_name", "type": "string", "default": ""},
            # 注释表字段的具体levelname，eg：Level1下Metabolism(对应的KO)
            {"name": "lowest_level", "type": "string", "default": ""},  # 注释表数据库对应的最低分类，eg：KEGG的ko
            {"name": "out_table", "type": "outfile", "format": "meta.otu.otu_table"},
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("anno_table").is_set and not self.option("gene_list").is_set:
            raise OptionError("请传入注释表格或者基因list文件！")
        if not self.option("geneset_table").is_set:
            raise OptionError("请传入基因丰度文件！")
        if self.option("level_type") == "" and self.option("anno_table").is_set:
            raise OptionError("请提供筛选的level水平")
        if self.option("level_type_name") != "":
            if self.option("lowest_level") == "":
                raise OptionError("请提供数据库对应的最低分类")

    def set_resource(self):
        self._cpu = 1
        self._memory = '2G'

    def end(self):
        super(CreateAbundTableAgent, self).end()


class CreateAbundTableTool(Tool):
    def __init__(self, config):
        super(CreateAbundTableTool, self).__init__(config)

    def create_abund_table(self):
        geneset_table_path = self.option("geneset_table").prop["path"]
        geneset_table = pd.read_table(geneset_table_path, sep='\t', header=0)
        new_otu_file_path = os.path.join(self.output_dir, "new_abund_table.xls")
        if self.option("gene_list").is_set:
            gene_list_path = self.option("gene_list").prop["path"]
            gene_list = pd.read_table(gene_list_path, sep='\t', header=0)
            gene_table = geneset_table.set_index("GeneID").ix[list(gene_list["GeneID"])]
            del gene_table["Total"]
            gene_table.to_csv(new_otu_file_path, sep="\t")
        else:
            anno_table_path = self.option("anno_table").prop["path"]
            anno_table = pd.read_table(anno_table_path, sep='\t', header=0)
            a = pd.DataFrame(anno_table)
            if self.option("level_type_name") != "":
                a = a.ix[:, ["#Query", self.option("lowest_level"), self.option("level_type")]]
                a.columns = ["GeneID", self.option("lowest_level"), self.option("level_type")]
            else:
                a = a.ix[:, ["#Query", self.option("level_type")]]
                a.columns = ["GeneID", self.option("level_type")]
            b = pd.DataFrame(geneset_table)        
            del b["Total"]
            abund = a.merge(b, on='GeneID', how='inner')
            level_type_abund = abund.drop(self.option("level_type"), axis=1).join(
                abund[self.option("level_type")].str.split(';', expand=True).stack().reset_index(
                    level=1, drop=True).rename(self.option("level_type")))
            if self.option("level_type_name") != "":
                if self.option("lowest_level") in ["Pathway"]:
                    pathway = abund[self.option("lowest_level")].str.split(";", expand=True).stack().reset_index(
                        level=1, drop=True).rename(self.option("lowest_level"))
                    level_type_abund[self.option("lowest_level")] = list(pathway)
                    level_type_abund = level_type_abund[level_type_abund[self.option("lowest_level")] != "-"]
                else:
                    level_type_abund = level_type_abund[level_type_abund[self.option("level_type")] == self.option(
                        "level_type_name")]
                    level_type_abund = level_type_abund[level_type_abund[self.option("lowest_level")] != "-"]
                level_type_abund_table = level_type_abund.groupby(self.option("lowest_level")).sum()
            else:
                level_type_abund = level_type_abund[level_type_abund[self.option("level_type")] != "-"]
                level_type_abund_table = level_type_abund.groupby(self.option("level_type")).sum()
            level_type_abund_table.to_csv(new_otu_file_path, sep="\t")

    def set_output(self):
        self.logger.info('开始设置输出结果文件')
        try:
            self.option('out_table', os.path.join(self.output_dir, "new_abund_table.xls"))
            self.logger.info(self.option('out_table').prop['path'])
        except Exception as e:
            raise Exception("输出结果文件异常——{}".format(e))

    def run(self):
        super(CreateAbundTableTool, self).run()
        self.create_abund_table()
        self.set_output()
        self.end()
