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


class GenesetClusterAction(RefRnaController):
    def __init__(self):
        super(GenesetClusterAction, self).__init__(instant=False)
    
    def GET(self):
        return 'khl'
    
    def POST(self):
        data=web.input()
        print data

        task_name = "ref_rna.report.geneset_cluster"
        task_type = "workflow"
        return_result = self.check_options(data, data.method)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param['submit_location']=data.submit_location
        my_param['type']=data.type # gene or transcript
        # my_param['distance_method']=data.distance_method # 距离算法
        my_param['method']=data.method #聚类方法 kmeans or hclust
        my_param['log']=data.log
        my_param['level']=data.level  # fpkm or tpm
        my_param['sub_num']=data.sub_num
        my_param['group_id']=data.group_id
        my_param['group_detail']= group_detail_sort(data.group_detail)
        my_param['express_method']=data.express_method # featurecounts or rsem
        my_param['geneset_id']=data.geneset_id
        my_param["task_type"]=task_type
        my_param['genes_distance_method'] = data.genes_distance_method
        if data.method == 'hclust':
            my_param['samples_distance_method'] = data.samples_distance_method
        # my_param['gene_cluster'] = data.gene_cluster #逻辑值为true/false
        # my_param['sample_cluster'] = data.sample_cluster #逻辑值为true/false
        
        geneset_info = {}
        for geneset in data.geneset_id.split(","):
            geneset_info = self.ref_rna.get_main_info(geneset, 'sg_geneset')
            if not geneset_info:
                info = {"success": False, "info": "geneset不存在，请确认参数是否正确！!"}
                return json.dumps(info)
        task_info = self.ref_rna.get_task_info(geneset_info['task_id'])
        
        main_table_name = "GenesetCluster_"+str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        mongo_data = [
                ('project_sn', task_info['project_sn']),
                ('task_id', task_info['task_id']),
                ('status', 'end'),
                ('type', data.type),
                ('name', main_table_name),
                ("desc","基因集聚类分析"),
                # ('gene_cluster',data.gene_cluster),
                # ('sample_cluster',data.sample_cluster),
                ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
            ]
        if data.genes_distance_method == '':
            mongo_data .append(("gene_cluster",False))
        else:
            mongo_data.append(('gene_cluster',True))
        if data.method == 'hclust':
            if data.samples_distance_method == '':
                mongo_data.append(('sample_cluster', False))
            else:
                mongo_data.append(("sample_cluster", True))
        collection_name = "sg_geneset_cluster"
        main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
        update_info = {str(main_table_id): collection_name}
        update_info = json.dumps(update_info)
        
        """获得表达量主表id"""
        
        express_id = self.ref_rna.get_express_id(task_info['task_id'], data.level, data.express_method)
        if not express_id:
             info = {"success": False, "info": "sg_express表params参数中没有找到表达量水平type:{},表达量计算软件express_method{}的表达量信息,请换一种表达量或表达量值类型".format(data.type,data.express_method)}
             return json.dumps(info)
        #express_info = self.ref_rna.get_main_info(ObjectId(express_id), 'sg_express') 

        options = {
            "express_file": str(express_id),
            "genes_distance_method": data.genes_distance_method,
            "log": data.log,
            "method": data.method,
            "group_id": data.group_id,
            "group_detail": data.group_detail,
            "type": data.type,  # 对应gene or transcript
            "express_method": data.express_method,
            "express_level": data.level, #对应fpkm or tpm
            "sub_num": data.sub_num,
            "geneset_cluster_id": str(main_table_id),
            "gene_list": data.geneset_id,
            "update_info": update_info
        }
        if data.method == 'hclust':
            options["samples_distance_method"] = data.samples_distance_method

        to_file = ["ref_rna.export_express_matrix_level(express_file)","ref_rna.export_group_table_by_detail(group_id)", "ref_rna.export_gene_list(gene_list)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,\
                task_id = task_info['task_id'],project_sn = task_info['project_sn'],\
                params = my_param, to_file = to_file)
        
        task_info = super(GenesetClusterAction, self).POST()
        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        print task_info
        return json.dumps(task_info)
    
    def check_options(self, data, method):
        """
        检查网页端传来的参数是否正确
        """
        if method == 'hclust':
            params_name = ['type','genes_distance_method','method','log','sub_num',"level",'samples_distance_method',
                        'group_id','group_detail','geneset_id', 'express_method','submit_location']
        if method == 'kmeans':
            params_name = ['type', 'genes_distance_method', 'method', 'log', 'sub_num', "level",
                           'group_id', 'group_detail', 'geneset_id',
                           'express_method', 'submit_location']
        success = []
        
        for names in params_name:
            if not hasattr(data, names):
                success.append("缺少参数！{}".format(names))
        
        geneset_id = str(data.geneset_id)
        if not isinstance(geneset_id, ObjectId) and not isinstance(geneset_id, types.StringType):
            success.append("传入的geneset_id {}不是一个ObjectId对象或字符串类型".format(geneset_id))
        return success
        
        
            
        

