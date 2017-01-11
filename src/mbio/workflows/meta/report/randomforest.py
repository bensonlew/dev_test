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
        
            {"name": "otutable", "type": "infile","format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "int", "default": 9},
            {"name": "grouptable", "type": "infile","format": "meta.otu.group_table"},
            
            {"name": "ntree", "type": "int", "default": 500},
            {"name": "problem_type", "type": "int", "default": 2},
            #{"name": "top_number", "type": "int", "default": 32767},
            {"name": "randomforest_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "group_id", "type": 'string'},
            {"name": "otu_id", "type": 'string'},
            {"name": "group_detail", "type": "string"}
        ]
        self.add_option(options)
        #newtable = os.path.join(self.work_dir, 'otutable1.xls')
        #f2 = open(newtable, 'w+')
        #with open(tablepath, 'r') as f:

        self.set_options(self._sheet.options())
        self.randomforest = self.add_tool("meta.beta_diversity.randomforest")
        #self.samples = re.split(',', self.option("samples"))
        self.output_dir = self.randomforest.output_dir

    def change_otuname(self, tablepath):
        newtable = os.path.join(self.work_dir, 'otutable1.xls')
        f2 = open(newtable, 'w+')
        with open(tablepath, 'r') as f:
            i = 0
            for line in f:
                if i == 0:
                    i = 1
                    f2.write(line)
                else:
                    line = line.strip().split('\t')
                    line_data = line[0].strip().split(' ')
                    line_he = "".join(line_data)
                    line[0] = line_he
                    #line[0] = line_data[-1]
                    for i in range(0, len(line)):
                        if i == len(line)-1:
                            f2.write("%s\n"%(line[i]))
                        else:
                            f2.write("%s\t"%(line[i]))
        f2.close()
        return newtable



    def run_randomforest(self):
        newtable = self.change_otuname(self.option('otutable').prop['path'])
        options = {
            'otutable': newtable,
            #'otutable':self.option('otutable'),
            'level': self.option('level'),
            #'envlabs':self.option('envlabs'),
            'grouptable':self.option('grouptable'),
            'ntree':self.option('ntree'),
            'problem_type':self.option('problem_type'),
            #'top_number':self.option('top_number')
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
            ["./randomforest_vimp_table.xls", "xls", "重要成分"],
            #["./randomforest_confusion_table.xls", "xls", "错误率"]
        ])
        super(RandomforestWorkflow, self).end()

    def set_db(self):
        api_randomforest = self.api.randomforest
        datadim = self.output_dir + '/randomforest_mds_sites.xls'
        datavip = self.output_dir + '/randomforest_vimp_table.xls'
        #dataerror = self.output_dir + '/randomforest_confusion_table.xls'
        if not os.path.isfile(datadim):
            raise Exception("找不到报告文件:{}".format(datadim))
        if not os.path.isfile(datavip):
            raise Exception("找不到报告文件:{}".format(datavip))
        #if not os.path.isfile(dataerror):
            #raise Exception("找不到报告文件:{}".format(dataerror))
        api_randomforest.add_randomforest_dim(file_path=datadim, table_id=self.option("randomforest_id"))
        api_randomforest.add_randomforest_vip(file_path=datavip, table_id=self.option("randomforest_id"))
        #api_randomforest.add_randomforest_error(file_path=dataerror, table_id=self.option("randomforest_id"))
        self.end()

    def run(self):
        self.run_randomforest()
        super(RandomforestWorkflow, self).run()        

