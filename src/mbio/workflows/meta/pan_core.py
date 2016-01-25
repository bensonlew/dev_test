# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""pan_core OTU计算"""

import os
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError


class PanCoreWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DatasplitWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type":"infile", 'format': "meta.otu.otu_table"}, 
            {"name": "group_table", "type":"infile", 'format': "meta.otu.group_table"}
        ]
