# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.environmental_regression_stat import EnvironmentalRegressionStat as G
from mainapp.libs.param_pack import *
import re


class EnvironmentalRegression(object):
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        params_name = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id', 'env_id', 'env_labs', 'PCAlabs_id']
        success = []
        print data
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!" % param}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {"success": False, "info": "level{}不在规定范围内{}".format(data.level_id)}
            return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = int(data.level_id)
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['submit_location'] = data.submit_location
        my_param['task_type'] = data.task_type
        my_param['group_id'] = data.group_id
        my_param['env_id'] = data.env_id
        my_param['env_labs'] = data.env_labs
        my_param['PCAlabs_id'] = data.PCAlabs_id
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "environmental_regression_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            environmental_regression_id = G().create_environmental_regression(params=params, group_id=data.group_id, from_otu_table=data.otu_id,name=name, level_id=data.level_id, env_id=data.env_id)
            print "test"
            #print network_id
            update_info = {str(environmental_regression_id): "sg_environmental_regression"}
            update_info = json.dumps(update_info)
            print update_info
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            print workflow_id
            (output_dir, update_api) = GetUploadInfo(client, member_id, otu_info['project_sn'], otu_info['task_id'], 'environmental_regression')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.environmental_regression",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": ["meta.export_otu_table_by_detail(otu_table)", "meta.export_group_table_by_detail(group_table)","env.export_float_env(envtable)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "otu_table": data.otu_id,
                    "group_table": data.group_id,
                    "group_detail": data.group_detail,
                    "update_info": update_info,
                    "level":int(data.level_id),
                    "envtable":data.env_id,
                    "env_labs":data.env_labs,
                    "PCAlabs":data.PCAlabs_id,
                    "environmental_regression_id": str(environmental_regression_id),
                }
            }
            print data.level_id
            print json_data
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

