# -*- coding: utf-8 -*-
# __author__ = 'konghualei, 20170421'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from bson import ObjectId
import datetime
import pandas as pd
import shutil
import re
from biocluster.workflow import Workflow

class ExpressPCAWorkflow(Workflow):
    """
    计算表达量相关性
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(ExpressPCAWorkflow, self).__init__(wsheet_object)
        options = [
            {"name":"express_file","type":"infile","format":"rna.express_matrix"},
            {"name":"group_id","type":"string"},
            {"name":"group_detail","type":"string"},
            {"name":"specimen","type":"string"},
            {"name":"pca_id","type":"string"},
            {"name":"update_info","type":"string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.pca = self.add_tool('meta.beta_diversity.pca')
        self.samples = re.split(',', self.option('specimen'))
    
    def run_pca(self,):
        """样本间pca分析"""
        new_fpkm = self.fpkm()
        opts = {
            "otutable": new_fpkm
        }
        self.pca.set_options(opts)
        self.pca.on('end', self.set_db)
        self.pca.run()
        
    def fpkm(self):
        fpkm = pd.read_table(self.option("express_file").prop['path'],sep="\t")
        print fpkm.columns
        no_samp = []
        sample_total = fpkm.columns[1:]
        for sam in sample_total:
            if sam not in self.samples:
                no_samp.append(sam)
        new_fpkm = fpkm.drop(no_samp, axis=1)
        print new_fpkm.columns
        self.new_fpkm = self.pca.work_dir + "/fpkm"
        header=['']
        header.extend(self.samples)
        new_fpkm.columns=header
        new_fpkm.to_csv(self.new_fpkm, sep="\t",index=False)
        return self.new_fpkm
    
    def set_db(self):
        api_pca = self.api.refrna_corr_express
        # corr_id = self.option("correlation_id")
        self.logger.info(self.pca.output_dir)
        _id = api_pca.add_pca_table(pca_path = self.pca.output_dir,express_id="58ef0bcba4e1af740ec4c14c",\
            detail=True, seq_type="gene")
        self.end()
    
    def run(self):
        self.run_pca()
        super(ExpressPCAWorkflow, self).run()
    
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".","","pca结果目录"]
        ])
        super(ExpressPCAWorkflow, self).end()
        
        
        
        