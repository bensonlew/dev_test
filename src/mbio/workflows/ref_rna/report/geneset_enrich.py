# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
from bson.objectid import ObjectId


class GenesetEnrichWorkflow(Workflow):
    """
    基因集富集分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(GenesetEnrichWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "kegg_table", "type": "string"},
            {"name": "go_list", "type": "string"},
            {"name": "genset_list", "type": "string"},
            {"name": "all_list", "type": "string"},
            {"name": "anno_type", "type": "string"},
            {"name": "geneset_type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_table_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "method", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.enrich_tool = self.add_tool("rna.go_enrich") if self.option("anno_type") == "go" else self.add_tool("rna.kegg_rich")
        self.output_dir = self.enrich_tool.output_dir
        # self.group_spname = dict()

    def run(self):
        if self.option("anno_type") == "kegg":
            options = {
                "kegg_table": self.option("kegg_table"),
                "all_list": self.option("all_list"),
                "diff_list": self.option("genset_list"),
                "correct": self.option("method")
            }
        else:
            options = {
                "diff_list": self.option("genset_list"),
                "all_list": self.option("all_list"),
                "go_list": self.option("go_list"),
                # "pval": self.option("pval"),
                "method": self.option("method"),
            }
        self.enrich_tool.set_options(options)
        self.enrich_tool.on('end', self.set_db)
        self.enrich_tool.run()
        super(GenesetEnrichWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        # api_geneset = self.api.geneset
        # output_file = self.output_dir+"/estimators.xls"
        # if not os.path.isfile(output_file):
        #     raise Exception("找不到报告文件:{}".format(output_file))
        # est_id = api_geneset.add_est_table(output_file, level=self.option('level'), otu_id=self.option('otu_id'), est_id=self.option("est_id"))
        # self.add_return_mongo_id('sg_alpha_diversity', est_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "基因集富集结果目录"],
            # ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        # print self.get_upload_files()
        super(GenesetEnrichWorkflow, self).end()

