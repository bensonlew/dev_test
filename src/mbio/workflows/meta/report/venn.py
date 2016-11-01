# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""Venn表计算"""

import os
import datetime
import json
import shutil
import re
from biocluster.workflow import Workflow


class VennWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(VennWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "update_info", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "samples", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "otu_id", "type": "string"},
            {"name": "venn_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.venn = self.add_tool("graph.venn_table")
        self.samples = re.split(',', self.option("samples"))

    def run_venn(self, no_zero_otu):
        options = {
            "otu_table": no_zero_otu,
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
        name = "venn_table_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        venn_id = api_venn.create_venn_table(self.sheet.params, myParams["group_id"], self.option("level"), self.option("otu_id"), name)
        venn_path = os.path.join(self.venn.work_dir, "venn_table.xls")
        venn_graph_path = os.path.join(self.venn.work_dir, "venn_graph.xls")
        api_venn.add_venn_detail(venn_path, venn_id, self.option("otu_id"), self.option("level"))
        api_venn.add_venn_graph(venn_graph_path, venn_id)
        self.add_return_mongo_id("sg_otu_venn", venn_id)
        self.end()

    def run(self):
        no_zero_otu = os.path.join(self.work_dir, "otu.nozero")
        my_sps = self.samples
        self.option("in_otu_table").sub_otu_sample(my_sps, no_zero_otu)
        num_lines = sum(1 for line in open(no_zero_otu))
        if num_lines < 11:
            raise Exception("Otu表里的OTU数目小于10个！请更换OTU表或者选择更低级别的分类水平！")
        self.run_venn(no_zero_otu)
        super(VennWorkflow, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["venn_table.xls", "xls", "Venn表格"]
        ])
        super(VennWorkflow, self).end()
