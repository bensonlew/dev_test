# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modify:20161124

"""无参转录组go富集分析"""

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
from bson.objectid import ObjectId


class GoEnrichWorkflow(Workflow):
    """
    报告中调用go富集分析时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(GoEnrichWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "enrich_file", "type": "string", "default": "none"},
            {"name": "pval", "type": "string", "default": "0.05"},               # 显著性水平
            {"name": "method", "type": "string", "default": "bonferroni,sidak,holm,fdr"}, # 多重校正方法
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.go_enrich = self.add_tool("denovo_rna.express.go_enrich")
        self.output_dir = self.go_enrich.output_dir

    def run_go_enrich(self):
        go_enrich_files = self.option("enrich_file").split(',')
        options = {
            "diff_list": go_enrich_files[1],
            "all_list": go_enrich_files[0],
            "go_list": go_enrich_files[2],
        }
        self.go_enrich.set_options(options)
        self.go_enrich.on("end", self.set_db)
        self.go_enrich.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [
            [".", "", "结果输出目录"],
            ["go_enrich_stat", "xls", "go富集分析统计表"],
            ["go_lineage*", "png", "显著性go有向无环图"],
        ]
        result_dir.add_regexp_rules([
            [r"go_enrich_*\.xls", "xls", "go富集分析统计表"],
            [r"go_lineage*", "png", "显著性go有向无环图"],
        ])
        result_dir.add_relpath_rules(relpath)
        super(GoEnrichWorkflow, self).end()

    def set_db(self):
        """
        保存结果表到mongo数据库中
        """
        api_go_enrich = self.api.denovo_go_enrich
        enrich_files = os.listdir(self.output_dir)
        for f in enrich_files:
            if re.search(r'go_enrich_*$', f):
                go_enrich_dir = os.path.join(self.output_dir, f)
            if re.search(r'go_lineage*$', f):
                go_graph_dir = os.path.join(self.output_dir, f)
            api_go_enrich.add_go_enrich(name=None, params=None, go_graph_dir, go_enrich_dir)
        self.end()

    def run(self):
        self.run_go_enrich()
        super(GoEnrichWorkflow, self).run()
