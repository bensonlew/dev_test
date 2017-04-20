# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
from bson.objectid import ObjectId


class GenesetClassWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(GenesetClassWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "geneset_go", "type": "string"},
            {"name": "geneset_cog", "type": "string"},
            {"name": "geneset_kegg", "type": "string"},
            {"name": "kegg_table", "type": "infile"},
            {"name": "anno_type", "type": "string"},
            {"name": "geneset_type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_table_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "geneset_id", "type": "string"},

        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        # self.group_spname = dict()

    def run(self):
        self.set_db()
        super(GenesetClassWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        # api_geneset = self.api.geneset
        output_file = self.option("geneset_cog")
        if not os.path.isfile(output_file):
            raise Exception("找不到报告文件:{}".format(output_file))
        print(output_file)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #     [".", "", "多样性指数结果目录"],
        #     ["./estimators.xls", "xls", "alpha多样性指数表"]
        # ])
        # print self.get_upload_files()
        super(GenesetClassWorkflow, self).end()

