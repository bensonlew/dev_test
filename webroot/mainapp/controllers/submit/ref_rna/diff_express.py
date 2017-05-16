# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng, modify by khl 20170428'
import web
import json
import random
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from biocluster.config import Config
from mainapp.models.mongo.submit.ref_rna.ref_diff import RefDiff
import types
from mainapp.models.mongo.meta import Meta
from mainapp.models.workflow import Workflow
from mainapp.controllers.project.ref_express_controller import RefExpressController
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from mbio.api.to_file.ref_rna import *

class DiffExpressAction(RefRnaController):
    def __init__(self):
        super(DiffExpressAction, self).__init__()
    
    def GET(self):
        return 'khl'
    
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        print data.type
        print 'haha'
        
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        
        task_type = 'workflow'
        task_name = 'ref_rna.report.diff_express'

        my_param['express_id'] = data.express_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['group_id'] = data.group_id
        my_param['control_id'] = data.control_id
        my_param['fc'] = data.fc
        my_param['pvalue_padjust'] = data.pvalue_padjust
        my_param['pvalue'] = data.pvalue
        my_param['diff_method'] = data.diff_method
        my_param['type'] = data.type
        my_param['task_type']= task_type
        my_param['submit_location'] = data.submit_location
        
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        express_info = self.ref_rna.get_main_info(data.express_id, 'sg_express')
        task_info = self.ref_rna.get_task_info(express_info['task_id'])
        express_params=json.loads(express_info["params"])
        print express_info
        print 'heihei'
        print task_info
        print 'heihei1'
        express_method = express_params["express_method"]
        value_type = express_params["type"]
        
        if express_info:
            main_table_name = "DiffExpress_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            task_id = express_info["task_id"]
            project_sn = express_info["project_sn"]

            mongo_data = [
                ('project_sn', task_info['project_sn']),
                ('task_id', task_info['task_id']),
                ('status', 'end'),
                ('name', main_table_name),
                ("value_type",value_type),
                ("express_id",ObjectId(data.express_id)),
                ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
            ]
            #if express_info["is_duplicate"] and express_info['trans'] and express_info['genes']:
            if "is_duplicate" in express_info.keys() and "trans" in express_info.keys() and "genes" in express_info.keys():
                mongo_data.extend([
                    ("id_duplicate", express_info["is_duplicate"]),
                    ("trans", express_info['trans']),
                    ('genes', express_info['genes'])
                ])   #参数值方便前端取数据
            else:
                raise Exception("{}没有is_duplicate,trans,genes信息".format(str(data.express_id)))
            collection_name = "sg_express_diff"
            main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
            update_info = {str(main_table_id): collection_name}
            update_info = json.dumps(update_info)
            try:
                class_code_id = self.ref_rna.get_class_code_id(task_id)
            except Exception:
                print "没有获得class_code_id信息"
            options = {
                "express_file": data.express_id,
                "update_info": update_info,
                "type":data.type,  #gene/ transcript
                "control_file": data.control_id,
                'fc': data.fc,
                "express_method": express_method,
                "group_id_id":data.group_id,
                'class_code': class_code_id,
                "diff_method":data.diff_method,
                "diff_express_id": str(main_table_id),
                "log":"None"
                # "group_id": data.group_id,
                # "group_detail":data.group_detail,
            }
            to_file = ["ref_rna.export_express_matrix_level(express_file)",  "ref_rna.export_control_file(control_file)", "ref_rna.export_class_code(class_code)"]
            if data.group_id != 'all':
                options.update({
                    "group_id": data.group_id,
                    "group_detail": data.group_detail,
                })
                to_file.append("ref_rna.export_group_table_by_detail(group_id)")
            self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name, 
                            to_file=to_file,params=my_param, project_sn=task_info['project_sn'], task_id=task_info['task_id'])
            task_info = super(DiffExpressAction, self).POST()
            task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
            print task_info
            return json.dumps(task_info)
        
        else:
            info = {"success": False, "info": "express_id不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['express_id', 'fc', 'group_detail', 'group_id', 'control_id', \
                        'submit_location','pvalue_padjust','pvalue','diff_method','type']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        for ids in [data.express_id, data.group_id, data.control_id]:
            ids = str(ids)
            print type(ids)
            if not isinstance(ids, ObjectId) and not isinstance(ids, types.StringTypes):
                success.append("传入的id：{}不是一个ObjectId对象或字符串类型".format(ids))
        return success
