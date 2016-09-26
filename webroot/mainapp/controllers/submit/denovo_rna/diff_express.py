# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from mainapp.config.db import get_mongo_client
import types
from biocluster.config import Config


class DiffExpress(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db_name = Config().MONGODB + '_rna'
        self.db = self.client[self.db_name]

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param['express_id'] = data.express_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['group_id'] = data.group_id
        my_param['control_id'] = data.control_id
        my_param['control_detail'] = group_detail_sort(data.control_detail)
        my_param['ci'] = data.ci
        my_param['rate'] = data.rate
        my_param['submit_location'] = data.submit_location
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        express_info = self.get_express_info(data.express_id)
        if express_info:
            name = "lefse_lda_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            task_info = self.get_task_info(express_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_id对应的表达量矩阵对应的task：{}没有member_id!".format(express_info["task_id"])}
                return json.dumps(info)
            lefse_id = G().create_species_difference_lefse(params, data.group_id, data.otu_id, name)
            update_info = {str(lefse_id): "sg_species_difference_lefse", 'database': self.db_name}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(express_info["task_id"], data.otu_id)
            (output_dir, update_api) = GetUploadInfo(client, member_id, express_info['project_sn'], express_info['task_id'], 'lefse_lda')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.lefse",
                "type": "workflow",
                "client": client,
                "project_sn": express_info["project_sn"],
                "to_file": ["meta.export_otu_table(otu_file)", "meta.export_cascading_table_by_detail(group_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "otu_file": data.otu_id,
                    "update_info": update_info,
                    "group_file": data.group_id,
                    "group_detail": data.group_detail,
                    "second_group_detail": data.second_group_detail,
                    "group_name": G().get_group_name(data.group_id, lefse=True, second_group=data.second_group_detail),
                    "strict": data.strict,
                    "lda_filter": data.lda_filter,
                    "lefse_id": str(lefse_id)
                }
            }
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
            info = {"success": False, "info": "express_id不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

    def get_express_info(self, express_id):
        if isinstance(express_id, types.StringTypes):
            express_id = ObjectId(express_id)
        elif isinstance(express_id, ObjectId):
            express_id = express_id
        else:
            raise Exception("输入express_id参数必须为字符串或者ObjectId类型!")
        collection = self.db['sg_denovo_express']
        express_info = collection.find_one({'_id': express_id})
        return express_info

    def get_task_info(self, task_id):
        sg_task = self.db['sg_task']
        result = sg_task.find_one({'task_id': task_id})
        return result

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['express_id', 'ci', 'group_detail', 'group_id', 'control_id', 'control_detail', 'rate', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if float(data.rate) >= 1 or float(data.rate) <= 0:
            success.append("差异基因比率rate不在范围内")
        if float(data.ci) >= 1 or float(data.ci) <= 0:
            success.append("显著性水平ci不在范围内")
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        control_detail = json.loads(data.control_detail)
        if not isinstance(control_detail, dict):
            success.append("传入的control_detail不是一个字典")
        for ids in [data.express_id, data.group_id, data.control_id]:
            if not isinstance(json.loads(ids), ObjectId):
                success.append("传入的id不是一个ObjectId对象")
        return success
