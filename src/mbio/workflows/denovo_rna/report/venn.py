# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""Venn表计算"""

import os
import json
import shutil
from biocluster.workflow import Workflow


class VennWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(VennWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "express_file", "type": "infile", 'format': "denovo_rna.express.express_matrix"},
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "express_id", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.venn = self.add_tool("graph.venn_table")

    def run_venn(self):
        options = {
            "otu_table": self.option("express_file"),
            "group_table": self.option("group_table")
        }
        self.venn.set_options(options)
        self.venn.on('end', self.set_db)
        self.venn.run()

    def set_db(self):
        sour = os.path.join(self.venn.work_dir, "output/venn_table.xls")
        dest = os.path.join(self.work_dir, "output")
        shutil.copy2(sour, dest)
        self.logger.info("正在往数据库里插入sg_otu_venn_detail表")
        api_venn = self.api.venn
        myParams = json.loads(self.sheet.params)
        venn_id = api_venn.add_denovo_venn(express_id=self.option('express_id'), venn_table=self.output_dir + '/venn_table.xls', venn_graph_path=self.venn.work_dir + '/venn_graph.xls', params=myParams)
        self.add_return_mongo_id("sg_otu_venn", venn_id)
        self.end()

    def run(self):
        self.run_venn()
        super(VennWorkflow, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["venn_table.xls", "xls", "Venn表格"]
        ])
        super(VennWorkflow, self).end()
