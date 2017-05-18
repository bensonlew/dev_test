# -*- coding: utf-8 -*-
# __author__ = 'konghualei' 20170420

"""Venn表计算"""

import os
import datetime
import json
import shutil
import re
from biocluster.workflow import Workflow
import pandas as pd

class ExpressVennWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(ExpressVennWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "express_file", "type":"string"},
            {"name": "group_id", "type":"string"},  #样本的分组信息
            {"name":"group_detail","type":"string"},
            {"name": "update_info", "type": "string"},
            {"name":"type","type":"string"},
            # {"name":"sample_group",'type':"string","default":"sample"},
            {"name":"venn_id","type":"string"},
        ]
        self.logger.info(options)
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.venn = self.add_tool("graph.venn_table")
        # self.samples = re.split(',', self.option("specimen"))
        
    def run_venn(self, fpkm_path, specimen):
        """样本间特异性基因venn图"""
        new_fpkm = self.get_sample_table(fpkm_path,specimen)
        options = {
            "otu_table": new_fpkm,
            "group_table": self.option("group_id")
        }
        
        self.logger.info("检查new_fpkm和new_group的路径:")
        self.logger.info(new_fpkm)
        self.venn.set_options(options)
        self.venn.on('end', self.set_db)
        self.venn.run()
        
    def set_db(self):
        venn_path = self.venn.output_dir
        api_venn = self.api.refrna_corr_express
        venn_id = self.option("venn_id")
        self.logger.info("准备开始向mongo数据库中导入venn图detail表和graph信息！")
        api_venn.add_venn_detail(venn_path + '/venn_table.xls', venn_id, 'ref')
        api_venn.add_venn_graph(venn_path + '/venn_graph.xls', venn_id, 'ref')
        self.logger.info("导入venn图detail表和graph表成功！")
        self.end()
    
    def get_samples(self):
        edger_group_path = self.option("group_id")
        self.logger.info(edger_group_path)
        self.samples=[]
        with open(edger_group_path,'r+') as f1:
            f1.readline()
            for lines in f1:
                line=lines.strip().split("\t")
                self.samples.append(line[0])
        print self.samples
        return self.samples
        
    def get_sample_table(self,fpkm_path, specimen):
        """ 根据筛选的样本名生成新的fpkm表 和 group_table表 """
        fpkm = pd.read_table(fpkm_path,sep="\t",)
        sample_name = fpkm.columns[1:]
        del_sam = []
        for sam in sample_name:
            try:
                if sam not in specimen:
                    del_sam.append(sam)
            except Exception:
                pass
        if del_sam:
            new_fpkm = fpkm.drop(del_sam, axis=1)
            self.new_fpkm = self.venn.work_dir + "/fpkm"
            header=['']
            header.extend(specimen)
            new_fpkm.columns = header
            new_fpkm.to_csv(self.new_fpkm, sep="\t",index=False)
            print 'end!'
            return self.new_fpkm
        else:
            return fpkm_path
        
    def run(self):
        fpkm = self.option("express_file").split(",")[0]
        specimen = self.get_samples()
        self.run_venn(fpkm, specimen)
        super(ExpressVennWorkflow, self).run()
    
    

        
        
