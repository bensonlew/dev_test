# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'

""""""

import datetime
from biocluster.workflow import Workflow
import re
import os
import json
import shutil


class NPcaWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(NPcaWorkflow, self).__init__(wsheet_object)
        options = [
        
            {"name": "otu_table", "type": "infile","format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "int", "default": 9},
            {"name": "second_group_table", "type": "infile","format": "meta.otu.group_table"},
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},
            #{"name": "env_labs", "type": "string", "default": ""},
            #{"name": "PCAlabs", "type": "string", "default": ""},
            {"name": "n_pca_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "second_group_id", "type": 'string'},
            {"name": "otu_id", "type": 'string'},
            {"name": "group_detail", "type": "string"},
            {"name": "second_group_detail", "type": "string"},
            {"name": "group_id","type":"string"},
        ]
        self.add_option(options)

        self.set_options(self._sheet.options())
        self.n_pca = self.add_tool("meta.beta_diversity.n_pca")
        self.output_dir = self.n_pca.output_dir

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



    def run_n_pca(self):
        newtable = self.change_otuname(self.option('otu_table').prop['path'])
        options = {
            'otu_table': newtable,
            'level': self.option('level'),
            'second_group_table':self.option('second_group_table'),
            'group_table':self.option('group_table'),            
        }
        self.n_pca.set_options(options)
        self.n_pca.on('end',self.set_db)
        self.output_dir = self.n_pca.output_dir
        self.n_pca.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "npca分析结果目录"],
            ["./sites.xls", "xls", "坐标数据"],
            ["./sd.xls", "xls", "方差大小"],
            ["./sdmax.xls", "xls", "置信上边界"],
            ["./sdmin.xls", "xls", "置信下边界"],
            ["./rotation_mean.xls", "xls", "平均值"],
            ["./importance.xls", "xls", "百分率返回"],
            ["./sitesall.xls", "xls", "所有信息"]
        ])
        super(NPcaWorkflow, self).end()

    def set_db(self):
        api_n_pca = self.api.n_pca
        datasite = self.output_dir + '/rotation_mean.xls'
        datamin = self.output_dir + '/sdmin.xls'
        datamax = self.output_dir + '/sdmax.xls'
        dataimportance = self.output_dir + '/importance.xls'
        datasitesall = self.output_dir + '/sitesall.xls'
        if not os.path.isfile(datasite):
            raise Exception("找不到报告文件:{}".format(datasite))
        if not os.path.isfile(datamin):
            raise Exception("找不到报告文件:{}".format(datamin))
        if not os.path.isfile(datamax):
            raise Exception("找不到报告文件:{}".format(datamax))
        if not os.path.isfile(dataimportance):
            raise Exception("找不到报告文件:{}".format(dataimportance))
        if not os.path.isfile(datasitesall):
            raise Exception("找不到报告文件:{}".format(datasiteall))
        #api_n_pca.add_n_pca_site(file_path=datasite, table_id=self.option("n_pca_id"))
        #api_n_pca.add_n_pca_min(file_path=datamin,table_id=self.option("n_pca_id"))
        #api_n_pca.add_n_pca_max(file_path=datamax,table_id=self.option("n_pca_id"))
        api_n_pca.add_n_pca_importance(file_path=dataimportance, table_id=self.option("n_pca_id"))
        api_n_pca.add_n_pca_sitesall(file_path=datasitesall, table_id=self.option("n_pca_id"))
        self.end()

    def run(self):
        self.run_n_pca()
        super(NPcaWorkflow, self).run()        

