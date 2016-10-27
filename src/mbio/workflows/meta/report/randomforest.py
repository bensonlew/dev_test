# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'

""""""

import datetime
from biocluster.workflow import Workflow
import re
import os
import json
import shutil


class RandomforestWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RandomforestWorkflow, self).__init__(wsheet_object)
        options = [
        
            {"name": "otutable", "type": "infile",
                "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "grouptable", "type": "infile", "format": "meta.otu.group_table"},
            
            {"name": "ntree", "type": "int", "default": 500},
            {"name": "problem_type", "type": "int", "default": 2},
            {"name": "top_number", "type": "int", "default": 50},
            {"name": "randomforest_id", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.randomforest = self.add_tool("meta.beta_diversity.randomforest")
        self.samples = re.split(',', self.option("samples"))



    def run_randomforest(self, no_zero_otu):
        options = {
            'otutable': no_zero_otu,
            'level': self.option('level'),
            'envlabs':self.option('envlabs'),
            'envtable':self.option('envtable'),
            'ntree':self.option('ntree'),
            'problem_type':self.option('problem_type'),
            'top_number':self.option('top_number')
        }
        self.randomforest.set_options(options)
        self.randomforest.on('end',self.set_db)
        self.output_dir = self.randomforest.output_dir
        self.randomforest.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出文件目录"],
            ["./randomforest_mds_sites.xls", "xls", "坐标数据"],
            ["./randomforest_vmip_table.xls", "xls", "重要成分"],
            ["./randomforest_confasion_table.xls", "xls", "错误率"]
        ])
        super(RandomforestWorkflow, self).end()

    def set_db(self):
        api_randomforest = self.api.randomforest
        datadim = self.output_dir + 'out/randomforest_mds_sites.xls'
        datavip = self.output_dir + 'out/randomforest_vimp_table.xls.xls'
        dataerror = self.output_dir + 'randomforest_confusion_table.xls'
        if not os.path.isfile(datadim):
            raise Exception("找不到报告文件:{}".format(datadim))
        if not os.path.isfile(datavip):
            raise Exception("找不到报告文件:{}".format(datavip))
        if not os.path.isfile(dataerror):
            raise Exception("找不到报告文件:{}".format(dataerror))
        api_randomforest.add_randomforest_dim(file_path=datadim, table_id=self.option("randomforest_id"))
        api_randomforest.add_randomforest_vip(file_path=datavip, table_id=self.option("randomforest_id"))
        api_randomforest.add_randomforest_error(file_path=dataerror, table_id=self.option("randomforest_id"))
        self.end()

    def run(self):
        self.run_randomforest()
        super(RandomforestWorkflow, self).run()        

