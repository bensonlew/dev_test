# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'

""""""

import datetime
from biocluster.workflow import Workflow
import re
import os
import json
import shutil


class RocWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RocWorkflow, self).__init__(wsheet_object)
        options = [
        
            {"name": "otu_table", "type": "infile","format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "int", "default": 9},
            {"name": "group_table", "type": "infile","format": "meta.otu.group_table"},
            
            {"name": "method", "type": "string", "default": "sum"},
            #{"name": "problem_type", "type": "int", "default": 2},
            {"name": "top_n", "type": "int", "default": 100},
            {"name": "roc_id", "type": "string"},
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
        self.roc = self.add_tool("meta.beta_diversity.roc")
        #self.samples = re.split(',', self.option("samples"))
        self.output_dir = self.roc.output_dir

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



    def run_roc(self):
        newtable = self.change_otuname(self.option('otu_table').prop['path'])
        options = {
            'otu_table': newtable,
            #'otutable':self.option('otutable'),
            'level': self.option('level'),
            #'envlabs':self.option('envlabs'),
            'group_table':self.option('group_table'),
            'method':self.option('method'),
            #'problem_type':self.option('problem_type'),
            'top_n':self.option('top_n')
        }
        self.roc.set_options(options)
        self.roc.on('end',self.set_db)
        self.output_dir = self.roc.output_dir
        self.roc.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "ROC分析结果目录"],   #modified by hongdongxuan at 91-92 20170324
            ["./roc_curve.xls", "xls", "ROC曲线结果表"],
            ["./roc_auc.xls", "xls", "AUC计算结果表"],
            ["./roc_plot_rocarea.xls", "xls", "置信区间"], # add 2 lines by hongdongxuan 20170324
            ["./roc_table.xls", "xls", "坐标数据"]
        ])
        super(RocWorkflow, self).end()

    def set_db(self):
        api_roc = self.api.roc
        datacurve = self.output_dir + '/roc_curve.xls'
        dataauc = self.output_dir + '/roc_auc.xls'
        if not os.path.isfile(datacurve):
            raise Exception("找不到报告文件:{}".format(datacurve))
        if not os.path.isfile(dataauc):
            raise Exception("找不到报告文件:{}".format(dataauc))
        api_roc.add_roc_curve(file_path=datacurve, table_id=self.option("roc_id"))
        api_roc.add_roc_auc(file_path=dataauc, table_id=self.option("roc_id"))
        self.end()

    def run(self):
        self.run_roc()
        super(RocWorkflow, self).run()        

