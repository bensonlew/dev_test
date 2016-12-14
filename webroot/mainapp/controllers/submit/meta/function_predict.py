# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

import web
import json
import datetime
import random
from mainapp.libs.param_pack import group_detail_sort
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.function_predict import FunctionPredict as FP
from mainapp.libs.param_pack import *
import re


class FunctionPredict(object):
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        params_name = ['otu_id', 'submit_location', "group_id", "group_detail"]
        success = []
        print data
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少{}参数".format(param)}
                return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['submit_location'] = data.submit_location
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            task_id = otu_info["task_id"]
            project_sn = otu_info["project_sn"]
            task_info = Meta().get_task_info(task_id)
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task:{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            predict_id = FP().add_function_predict(name=None, params=params, otu_id=data.otu_id)
            update_info = {str(predict_id): "sg_16s_function_prediction"}
            update_info = json.dumps(update_info)
            (output_dir, update_api) = GetUploadInfo(client, member_id, project_sn, task_id, '16s_function_predict')
            workflow_id = self.get_new_id(task_id, data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.function_predict",
                "type": "workflow",
                "client": client,
                "project_sn": project_sn,
                "to_file": ["function_predict.export_otu_table_by_detail(otu_table)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "update_info": update_info,
                    "otu_table": data.otu_id,
                    "group_detail": data.group_detail,
                    "predict_id": str(predict_id)
                }
            }
            insert_data = {
                "client": client,
                "workflow_id": workflow_id,
                "json": json.dumps(json_data),
                "ip": web.ctx.ip
            }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
