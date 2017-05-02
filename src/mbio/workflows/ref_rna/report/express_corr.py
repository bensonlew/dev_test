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

class ExpressCorrWorkflow(Workflow):
    """
    计算表达量相关性
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(ExpressCorrWorkflow, self).__init__(wsheet_object)
        options = [
            {"name":"express_file","type":"string"},
            {"name":"group_id","type":"string"},
            {"name":"group_detail","type":"string"},
            {"name":"correlation_id","type":"string"},
            {"name":"update_info","type":"string"},
            {"name":"type","type":"string","default":"gene"}, #传给to_file 参数
            {"name":"method","type":"string","default":"pearson"}, #聚类方法
            {"name":"hclust_method",'type':"string",'default':'complete'}, #层次聚类方式
            {"name":"corr_pca","type":"string"} #pca 或 correlation分析
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        if self.option("corr_pca") == "corr":
            self.corr = self.add_tool('denovo_rna.mapping.correlation')
        if self.option("corr_pca") == "pca":
            self.pca = self.add_tool('meta.beta_diversity.pca')
        
    def run_corr(self):
        """样本间相关性分析"""
        specimen = self.get_samples()
        new_fpkm = self.fpkm(specimen)
        opts = {
            "fpkm":new_fpkm,
            "method":self.option('method'),
            "hclust_method":self.option("hclust_method")
        }
        self.corr.set_options(opts)
        self.corr.on('end', self.set_db)
        self.corr.run()
    
    def get_samples(self):
        edger_group_path = self.option("group_id")
        self.logger.info(edger_group_path)
        samples=[]
        with open(edger_group_path,'r+') as f1:
            f1.readline()
            for lines in f1:
                line=lines.strip().split("\t")
                samples.append(line[0])
        print samples
    
    def run_pca(self):
        """样本间pca分析"""
        specimen = self.get_samples()
        new_fpkm = self.fpkm(specimen)
        opts = {
            "otutable": new_fpkm
        }
        self.pca.set_options(opts)
        self.pca.on('end', self.set_db)
        self.pca.run()
        
    def fpkm(self,samples):
        fpkm_path = self.option("express_file").split(",")[0]
        fpkm = pd.read_table(fpkm_path, sep="\t")
        print fpkm.columns
        no_samp = []
        sample_total = fpkm.columns[1:]
        for sam in sample_total:
            try:
                if sam not in samples:
                    no_samp.append(sam)
            except Exception:
                pass
        if no_samp:
            new_fpkm = fpkm.drop(no_samp, axis=1)
            print new_fpkm.columns
            if self.option("corr_pca") == 'corr':
                self.new_fpkm = self.corr.work_dir + "/fpkm"
            if self.option("corr_pca") == "pca":
                self.new_fpkm = self.pca.work_dir + "/fpkm"
            header=['']
            header.extend(self.samples)
            new_fpkm.columns=header
            new_fpkm.to_csv(self.new_fpkm, sep="\t",index=False)
            return self.new_fpkm
        else:
            return fpkm_path
    
    def set_db(self):
        api_corr = self.api.refrna_corr_express
        if self.option('corr_pca') == 'corr':
            self.logger.info(self.corr.output_dir)
            # _id = api_corr.add_correlation_table(self.corr.output_dir,express_id="58ef0bcba4e1af740ec4c14c",\
                # detail=False, seq_type="gene")
            api_corr.add_correlation_detail(self.corr.output_dir,self.option("correlation_id"),updata_tree=True)
        if self.option('corr_pca') == 'pca':
            self.logger.info(self.pca.output_dir)
            _id = api_corr.add_pca_table(pca_path = self.pca.output_dir,express_id="58ef0bcba4e1af740ec4c14c",\
                detail=True, seq_type="gene")
        self.end()
        
    def run(self):
        if self.option("corr_pca") == "corr":
            self.run_corr()
        if self.option("corr_pca") == "pca":
            self.run_pca()
        super(ExpressCorrWorkflow, self).run()

        
        
        
        