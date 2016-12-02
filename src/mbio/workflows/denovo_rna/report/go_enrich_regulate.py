# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modify:20161118

"""无参转录组go富集分析/go调控分析"""

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
import shutil
from bson.objectid import ObjectId


class GoEnrichRegulateWorkflow(Workflow):
    """
    交互分析时调用go富集分析/go调控分析时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(GoEnrichRegulateWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "enrich_file", "type": "string", "default": "none"},
            {"name": "regulate_file", "type": "string", "default": "none"},
            {"name": "pval", "type": "string", "default": "0.05"},                        # 显著性水平
            {"name": "method", "type": "string", "default": "bonferroni,sidak,holm,fdr"}, # 多重校正方法
            {"name": "go_enrich_id", "type": "string"},
            {"name": "go_regulate_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "analysis_type", "type": "string"},    # 分析类型(enrich/regulate/stat)
            {"name": "express_id", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.go_enrich = self.add_tool("denovo_rna.express.go_enrich")
        self.go_regulate = self.add_tool("denovo_rna.express.go_regulate")
        self.enrich_output = self.go_enrich.output_dir
        self.regulate_output = self.go_regulate.output_dir

    def run_go_enrich(self):
        go_enrich_files = self.option("enrich_file").split(',')
        options = {
            "diff_list": go_enrich_files[1],
            "all_list": go_enrich_files[0],
            "go_list": go_enrich_files[2],
            "pval": self.option("pval"),
            "method": self.option("method"),
        }
        self.go_enrich.set_options(options)
        self.go_enrich.on("end", self.set_db)
        self.go_enrich.run()

    def run_go_regulate(self):
        go_regulate_files = self.option("regulate_file").split(',')
        options = {
            "diff_stat": go_regulate_files[0],
            "go_level_2": go_regulate_files[1],
        }
        self.go_regulate.set_options(options)
        self.go_regulate.on("end", self.set_db)
        self.go_regulate.run()

    def set_output(self):
        if self.option("analysis_type") == "enrich":
            for enrich_file in os.listdir(self.enrich_output):
                enrich = os.path.join(self.enrich_output, enrich_file)
                os.system("cp " + enrich + " " + self.output_dir)
        if self.option("analysis_type") == "regulate":
            for regulate_file in os.listdir(self.regulate_output):
                regulate = os.path.join(self.regulate_output, regulate_file)
                os.system("cp " + regulate + " " + self.output_dir)
        if self.option("analysis_type") == "stat":
            for enrich_file in os.listdir(self.enrich_output):
                enrich = os.path.join(self.enrich_output, enrich_file)
                os.system("cp " + enrich + " " + self.output_dir)
            for regulate_file in os.listdir(self.regulate_output):
                regulate = os.path.join(self.regulate_output, regulate_file)
                os.system("cp " + regulate + " " + self.output_dir)

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [
            [".", "", "结果输出目录"],
            ["go_enrich_stat", "xls", "go富集分析统计表"],
            ["go_regulate", "xls", "基因上下调在Go2level层级分布情况表"],
            ["go_lineage*", "png", "显著性go有向无环图"],
        ]
        result_dir.add_regexp_rules([
            [r"go_enrich_.*", "xls", "go富集结果文件"],
            [r"Go_regulate.xls", "xls", "上下调基因go注释"],
            [r"go_lineage.*", "png", "go富集有向无环图"],
        ])
        result_dir.add_relpath_rules(relpath)
        super(GoEnrichRegulateWorkflow, self).end()

    def set_db(self):
        """
        保存结果表到mongo数据库中
        """
        api_go_enrich = self.api.denovo_go_enrich
        output_files = os.listdir(self.output_dir)
        for f in output_files:
            if re.search(r'go_enrich_*\.xls$', f):
                go_enrich_dir = os.path.join(self.output_dir, f)
            if re.search(r'Go_regulate.xls$', f):
                go_regulate_dir = os.path.join(self.output_dir, f)
            if re.search(r'go_lineage*$', f):
                go_graph_dir = os.path.join(self.output_dir, f)
        if self.option("analysis_type") == "enrich":
            api_go_enrich.add_go_enrich()
        if self.option("analysis_type") == "regulate":
            api_go_enrich.add_go_regulate()
        if self.option("analysis_type") == "stat":
            api_go_enrich.add_go_enrich()
            api_go_enrich.add_go_regulate()
        self.end()

    def run(self):
        if self.option("analysis_type") == "enrich":
            self.run_go_enrich()
        if self.option("analysis_type") == "regulate":
            self.run_go_regulate()
        if self.option("analysis_type") == "stat":
            self.run_go_enrich()
            self.run_go_regulate()
        super(GoEnrichRegulateWorkflow, self).run()
        self.set_output()
