# -*- coding: utf-8 -*-
# __author__ = 'xuting'  last modify by qindanhua 20170110
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.ref_express_controller import RefExpressController
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from mbio.api.to_file.ref_rna import *
from bson import ObjectId

class ExpressPcaAction(RefRnaController):
    def __init__(self):
        super(ExpressPcaAction, self).__init__(instant=True)
    
    def GET(self):
        return 'khl'
    
    def POST(self):
        data = web.input()
        postArgs = ['group_id','group_detail','submit_location','express_id']
        
        print data.express_id
        print data.group_detail
        print data.group_id
        
        for args in postArgs:
            if not hasattr(data,args):
                info = {'success': False, 'info': '%s参数缺少!' % args}
                return json.dumps(info)
        
        task_name = 'ref_rna.report.express_corr'
        task_type = 'workflow'
        
        my_param = {}
        my_param['express_id'] = data.express_id
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = data.group_detail
        my_param['submit_location'] = data.submit_location
        my_param["task_type"]=task_type
        
        express_info = self.ref_rna.get_main_info(data.express_id, 'sg_express')
        task_info = self.ref_rna.get_task_info(express_info['task_id'])

        if express_info:
            main_table_name = "ExpressPCA_"+datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
            group_detail_dict = json.loads(data.group_detail)
            
            mongo_data = [
                ('project_sn', task_info['project_sn']),
                ('task_id', task_info['task_id']),
                ('status', 'start'),
                ('name', main_table_name),
                ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
            ]
            
            collection_name = "sg_express_pca"
            main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
            update_info = {str(main_table_id): collection_name}
            
            specimen_ids = list()
            for v in group_detail_dict.values():
                for tmp in v:
                    specimen_ids.append(tmp)
            
            specimen_ids = ",".join(specimen_ids)
            
            options = {
                "express_file": data.express_id,
                "correlation_id": main_table_id,
                "group_id": data.group_id,
                "group_detail": data.group_detail,
                "corr_pca": "pca",
                "type": "gene",
                "update_info": json.dumps(update_info)
            }
            
            to_file = ["ref_rna.export_express_matrix_level(express_file)", "ref_rna.export_group_table_by_detail(group_id)"]
            self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,\
                task_id = express_info['task_id'], project_sn = express_info['project_sn'],\
                params = my_param, to_file = to_file)
            task_info = super(ExpressPcaAction,self).POST()

            task_info['content'] = {
                'ids':{
                    'id':str(main_table_id),
                    'name':main_table_name
                }}
            return json.dumps(task_info)
        else:
            info = {"success": False, "info": "表达量表不存在，请确认参数是否正确！!"}
            return json.dumps(info)
            