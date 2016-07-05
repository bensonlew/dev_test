# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""pan_core OTU计算"""

import shutil
import os
import json
import datetime
from biocluster.workflow import Workflow


class PanCoreWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(PanCoreWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "update_info", "type": "string"},
            {"name": "category_name", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "pan_id", "type": "string"},
            {"name": "core_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.pan_core = self.add_tool("meta.otu.pan_core_otu")

    def run_pan_core(self):
        if self.option("group_table").prop["is_empty"]:
            options = {
                "in_otu_table": self.option("in_otu_table")
            }
        else:
            options = {
                "in_otu_table": self.option("in_otu_table"),
                "group_table": self.option("group_table")
            }
        self.pan_core.set_options(options)
        self.pan_core.on('end', self.set_db)
        self.pan_core.run()

    def set_db(self):
        sour = os.path.join(self.pan_core.work_dir, "output/core.richness.xls")
        dest = os.path.join(self.work_dir, "output")
        shutil.copy2(sour, dest)
        sour = os.path.join(self.pan_core.work_dir, "output/pan.richness.xls")
        shutil.copy2(sour, dest)
        self.logger.info("正在写入mongo数据库")
        api_pan_core = self.api.pan_core
        name = "pan_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        myParams = json.loads(self.sheet.params)
        pan_id = api_pan_core.create_pan_core_table(1, self.sheet.params, myParams["group_id"], self.option("level"), myParams["otu_id"], name)
        core_id = api_pan_core.create_pan_core_table(2, self.sheet.params, myParams["group_id"], self.option("level"), myParams["otu_id"], name)
        pan_path = self.pan_core.option("pan_otu_table").prop['path']
        core_path = self.pan_core.option("core_otu_table").prop['path']
        api_pan_core.add_pan_core_detail(pan_path, pan_id)
        api_pan_core.add_pan_core_detail(core_path, core_id)
        self.add_return_mongo_id('sg_otu_pan_core', pan_id)
        self.add_return_mongo_id('sg_otu_pan_core', core_id)
        self.end()

    def run(self):
        self.run_pan_core()
        super(PanCoreWorkflow, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["core.richness.xls", "xls", "core 表格"],
            ["pan.richness.xls", "xls", "pan 表格"]
        ])
        print self.get_upload_files()
        super(PanCoreWorkflow, self).end()
