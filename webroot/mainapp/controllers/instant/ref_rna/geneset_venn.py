# -*- coding: utf-8 -*-
# __author__ = 'khl'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.ref_express_controller import RefExpressController
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from mbio.api.to_file.ref_rna import *
from bson import ObjectId

class GenesetVennAction(RefRnaController):
    def __init__(self):
        super(GenesetVennAction, self).__init__(instant=True)
    
    def GET(self):
        return 'khl'
    
    def POST(self):
        data=web.input()
        postArgs = ['type','geneset_id','submit_location',"task_id"]
        for arg in postArgs:
            if not hasattr(data,arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        task_name = 'ref_rna.report.geneset_venn'
        task_type = 'workflow'
        
        my_param = dict()
        my_param['type']=data.type
        my_param['geneset_id']=data.geneset_id
        my_param['submit_location'] = data.submit_location
        my_param["task_id"] = data.task_id
        my_param['task_type'] = task_type

        # 判断传入的基因集id是否存在
        geneset_info = {}
        for geneset in data.geneset_id.split(","):
            geneset_info = self.ref_rna.get_main_info(geneset, 'sg_geneset')
            if not geneset_info:
                info = {"success": False, "info": "geneset不存在，请确认参数是否正确！!"}
                return json.dumps(info)
        task_info = self.ref_rna.get_task_info(geneset_info['task_id'])
        if task_info:
            geneset_num=len(data.geneset_id.split(","))
            if geneset_num<=1 or geneset_num >=7:
                info = {"success": False, "info": "进行Venn分析，分组方案的分组类别必须大于等于2且小于等于6！"}
                return json.dumps(info)
            
            main_table_name = 'GenesetVenn_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
            collection_name = 'sg_geneset_venn'
            mongo_data = [
                ('project_sn', task_info['project_sn']),
                ('task_id', task_info['task_id']),
                ('status', 'end'),
                ('desc',"基因集venn图分析"),
                ('name', main_table_name),
                ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
            ]
            main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
            update_info = {str(main_table_id): 'sg_geneset_venn'}
           
            options = {
                "geneset_file":data.geneset_id,
                "update_info":json.dumps(update_info),
                "type":data.type,
                "geneset_venn_id":str(main_table_id)
            }
            to_file = 'ref_rna.export_geneset_venn_level(geneset_file)'
            self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,\
                    task_id = task_info['task_id'],project_sn = task_info['project_sn'],\
                    params = my_param, to_file = to_file)
            task_info = super(GenesetVennAction,self).POST()
            
            task_info['content'] = {
                    'ids':{
                        'id':str(main_table_id),
                        'name':main_table_name
                    }}
            return json.dumps(task_info)
        else:
            info = {"success": False, "info": "基因集对应task_id不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        
