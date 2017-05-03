#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ = 'konghualei,20170426'

import web
import json
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from biocluster.config import Config
from mainapp.models.mongo.submit.denovo_rna.denovo_cluster import DenovoExpress
import types
from mainapp.models.mongo.meta import Meta
from mainapp.models.workflow import Workflow
from mainapp.controllers.project.denovo_controller import DenovoController
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.ref_express_controller import RefExpressController
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from mbio.api.to_file.ref_rna import *
from bson import ObjectId
from mainapp.models.mongo.instant.ref_rna.geneset_venn_cluster import GeneSetVennCluster

class GenesetCluster(RefRnaController):
    def __init__(self):
        super(GenesetCluster, self).__init__(instant=False)
    
    def POST(self):
        data=web.input()
        print data
        return_result = self.check_options(data)
        
        task_name = "ref_rna.export.geneset_cluster"
        task_type = "workflow"
        
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        
        my_param = dict()
        my_param['submit_location']=data.submit_location
        my_param['type']=data.type # fpkm or tpm
        my_param['distance_method']=data.distance_method # 距离算法
        my_param['method']=data.method #聚类方法 kmeans or hclust
        my_param['log']=data.log
        my_param['level']=data.level  # gene or transcript
        my_param['sub_num']=data.sub_num
        my_param['group_id']=data.group_id
        my_param['group_detail']=data.group_detail
        my_param['express_method']=data.express_method # featurecounts or rsem
        my_param['geneset_id']=data.geneset_id
        my_param["task_type"]=task_type
        
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        geneset_info = self.ref_rna.get_main_info(geneset, 'sg_geneset')
        task_info = self.ref_rna.get_task_info(geneset_info['task_id'])
        
        main_table_name = "GenesetCluster_"+str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        main_table_id = GeneSetVennCluster().add_genesetCluster(task_info, params=my_param,type=data.type)
        
        update_info = {str(cluster_id): 'sg_geneset_cluster'}
        update_info = json.dumps(update_info)
        
        """获得表达量主表id"""
        try:
            express_id = RefExpressController(instant=False).get_express_id(task_info['task_id'],data.type,data.express_method)
        raise Exception:
            print "根据task_id:{}、type:{}、express_method:{}未获得表达量的主表id".format(task_info['task_id'],data.type,data.express_method)
        
        """获得选中的样品名称信息"""
        group_detal_dict = json.loads(data.group_detail)
        
        specimen_ids = list()
        for v in group_detal_dict.values():
            for tmp in v:
                specimen_ids.append(tmp)
        specimen_ids = ",".join(specimen_ids)
        
        options = {
            "express_file": express_id,
            "distance_method": data.distance_method,
            "log": data.log,
            "method": data.method,
            "group_id": data.group_id,
            "group_detail": data.group_detail,
            "specimen": self.meta.sampleIdToName(specimen_ids),
            "type": data.type,
            "express_method": data.express_method,
            "level": data.level,
            "sub_num": data.sub_num,
            "geneset_cluster_id": main_table_id,
            "gene_list": data.geneset_id,
            "update_info": update_info
        }
        
        to_file = ["ref_rna.export_express_matrix(express_file)","ref_rna.export_geneset_list(gene_list)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,\
                task_id = task_info['task_id'],project_sn = task_info['project_sn'],\
                params = my_param, to_file = to_file)
        
        task_info = super(GenesetCluster, self).POST()
        task_info['content'] = {'ids': {'id': str(cluster_id), 'name': main_table_name}}
        print task_info
        return json.dumps(task_info)
    
    def check_options(self, data):
        """
        检查网页端传来的参数是否正确
        """
        params_name = ['type','distance_method','method','log','sub_num',"level",\
                    'group_id','group_detail','geneset_id',\
                    'express_method','submit_location']
        success = []
        for names in params_name:
            if not hasattr(data, names):
                success.append("缺少参数！")
        geneset_id = str(data.geneset_id)
        if not isinstance(geneset_id, ObjectId) and not isinstance(geneset_id, types.StringType):
            success.append("传入的geneset_id {}不是一个ObjectId对象或字符串类型".format(geneset_id))
        return success
        
        
            
        

