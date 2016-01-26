# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""pan_core OTU计算"""

import os
from bson.objectid import ObjectId
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError


class PanCoreWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DatasplitWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", 'format': "meta.otu.otu_table"}, 
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "category_name", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "pan_id", "type": "string"},
            {"name": "core_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.pan_core = self.add_tool("meta.otu.pan_core")

    def run_pan_core(self):
        options = {
            "in_otu_table": self.option("in_otu_table"),
            "group_table": self.option("group_table")
        }
        self.pan_core.set_options(options)
        self.on_rely(self.run_pan_core, self.set_db)
        self.pan_core.run()

    def set_db(self):
        api_pan_core = self.api.pan_core
        pan_path = 


