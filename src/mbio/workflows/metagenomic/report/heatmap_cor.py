# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
from biocluster.workflow import Workflow
import glob
import os
from bson import ObjectId
import re


class HeatmapCorWorkflow(Workflow):
    """
    报告中计算相关性系数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(HeatmapCorWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "geneset_id", "type": "string"},
            {"name": "anno_id", "type": "string"},
            {"name": "level_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "env_id", "type": "string"},
            {"name": "env_labs", "type": "string"},
            {"name": "abund_file", "type": "infile", 'format': "meta.otu.otu_table"}, # 当丰度文件已存在时
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "gene_list", "type": "infile", "format": "meta.profile"},
            {"name": "level_type", "type": "string", "default": ""},
            {"name": "level_type_name", "type": "string", "default": ""},
            {"name": "lowest_level", "type": "string", "default": ""},  # 注释表数据库对应的最低分类，eg：KEGG的ko
            {"name": "envtable", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "level", "type": "int"},
            {"name": "correlation_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "method", "type": "string", "default": "pearsonr"},
            {"name": "env_cluster", "type": "string", "default": "average"},
            {"name": "species_cluster", "type": "string", "default": "average"},
            {"name": "group_detail", "type": "string"},
            {"name": "top_species", "type": "int", "default": 50},  # add new option (flit top N species)
            {"name": "main_id", "type": "string"},
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.correlation = self.add_tool('statistical.pearsons_correlation')

    def run_get_abund_table(self):
        self.get_abund_table = self.add_tool('meta.create_abund_table')
        self.get_abund_table.set_options({
        'anno_table': self.option('anno_table'),
        'geneset_table': self.option('geneset_table'),
        'gene_list': self.option('gene_list'),
        'level_type': self.option('gene_list'),
        'level_type_name': self.option('level_type_name'),
        'lowest_level': self.option('lowest_level'),
        })
        self.get_abund_table.on("end", self.run_correlation)
        self.get_abund_table.run()

    def run_correlation(self):
        env_cluster = self.option("env_cluster")
        species_cluster = self.option("species_cluster")
        if self.option("abund_file").is_set:
            abund_table = self.option("abund_file")
        else:
            abund_table = self.get_abund_table.option("out_table")
        self.correlation.set_options({
            'otutable': abund_table,
            'envtable': self.option('envtable'),
            "method": self.option('method'),
            "env_cluster": env_cluster,
            "species_cluster": species_cluster,
            "top_species": self.option('top_species')
            })
        self.correlation.on("end", self.set_db)
        self.correlation.run()

    def run(self):
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        if self.option("abund_file").is_set:
            self.run_correlation()
        else:
            self.run_get_abund_table()
        super(HeatmapCorWorkflow, self).run()

    def set_db(self):
        """
        保存结果表到mongo数据库中
        """
        self.logger.info("正在写入mongo数据库")
        species_tree = ""
        env_tree = ""
        api_correlation = self.api.heatmap_cor
        corr_path = glob.glob(self.correlation.output_dir+"/*correlation*")
        pvalue_path = glob.glob(self.correlation.output_dir+"/*pvalue*")
        if self.option("species_cluster") != "":
            species_tree = self.correlation.work_dir + "/species_tree.tre"
        if self.option("species_cluster") != "":
            env_tree = self.correlation.work_dir + "/env_tree.tre"
        api_correlation.add_heatmap_cor_detail(corr_path[0], "correlation", self.option("main_id"))
        api_correlation.add_heatmap_cor_detail(pvalue_path[0], "pvalue", self.option("main_id"), 
                                               species_tree=species_tree, env_tree=env_tree)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.correlation.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "相关性Heatmap分析结果目录"],
            ["./pearsons_correlation_at_otu_level.xls", "xls", "相关性系数表"],
            ["./pearsons_pvalue_at_otu_level.xls", "xls", "相关性P值"]
        ])
        super(HeatmapCorWorkflow, self).end()
