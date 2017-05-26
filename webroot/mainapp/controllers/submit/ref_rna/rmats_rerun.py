# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/5/12 09:59

import re, os, Bio, argparse, sys, fileinput, urllib2
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


class RmatsRerunAction(RefRnaController):
    def __init__(self):
        super(RmatsRerunAction, self).__init__()
    
    def GET(self):
        return 'jlf'
    
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        # print data.express_id
        # print data.group_detail
        # print data.control_id
        # print data.group_id
        # print data.fc
        # print data.pvalue_padjust
        # print data.diff_method
        # print data.pvalue
        # print data.diff_method
        # print data.type
    
        # return "diff_express"
    
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
    
        task_type = 'workflow'
        task_name = 'ref_rna.report.diff_express'
    
        my_param['splicing_id'] = data.splicing_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['group_id'] = data.group_id
        my_param['control_id'] = data.control_id
        my_param['fc'] = data.fc
        
        my_param['diff_method'] = data.diff_method
        my_param['type'] = data.type
        my_param['task_type'] = task_type
        my_param['submit_location'] = data.submit_location
    
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        express_info = self.ref_rna.get_main_info(data.express_id, 'sg_express')
        task_info = self.ref_rna.get_task_info(express_info['task_id'])
    
        if express_info:
            main_table_name = "DiffExpress_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            task_id = express_info["task_id"]
            project_sn = express_info["project_sn"]
        
            mongo_data = [
                ('project_sn', task_info['project_sn']),
                ('task_id', task_info['task_id']),
                ('status', 'start'),
                ('name', main_table_name),
                ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
            ]
            collection_name = "sg_express_diff"
            main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
            update_info = {str(main_table_id): collection_name}
            update_info = json.dumps(update_info)
        
            options = {
                "express_file": data.express_id,
                "update_info": update_info,
                "group_id": data.group_id,
                "group_detail": data.group_detail,
                "type": data.type,
                "control_file": data.control_id,
                'fc': data.fc,
                "diff_method": data.diff_method,
                "diff_express_id": str(main_table_id)
            }
            to_file = ["ref_rna.export_express_matrix_level(express_file)", "ref_rna.export_control_file(control_file)"]
            if data.group_id != 'all':
                to_file.append("ref_rna.export_group_table_by_detail(group_file)")
                options.update({
                    "group_file": data.group_id,
                    "group_detail": data.group_detail,
                })
            self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                                module_type='workflow', to_file=to_file, main_id=diff_express_id,
                                collection_name="sg_ref_express_diff")
            task_info = super(DiffExpressAction, self).POST()
            task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
            print task_info
            return json.dumps(task_info)
    
        else:
            info = {"success": False, "info": "express_id不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        pass
