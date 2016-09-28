# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from collections import OrderedDict
from biocluster.config import Config
from mbio.api.database.denovo_express import *
from mainapp.models.mongo.denovo import Denovo


class DiffExpress(object):
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

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
        my_param['control_detail'] = OrderedDict(sorted(json.loads(data.control_detail).items(), key=lambda t: t[0]))
        my_param['ci'] = data.ci
        my_param['rate'] = data.rate
        my_param['submit_location'] = data.submit_location
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        express_info = Denovo().get_main_info(data.express_id)
        if express_info:
            task_info = Denovo().get_task_info(express_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_id对应的表达量矩阵对应的task：{}没有member_id!".format(express_info["task_id"])}
                return json.dumps(info)
            express_id = DenovoExpress().add_express_diff(params=params, major=False, samples=samples, compare_column=compare_column)
            update_info = {str(express_id): "sg_species_difference_lefse", 'database': self.db_name}
            update_info = json.dumps(update_info)
            workflow_id = Denovo().get_new_id(express_info["task_id"], data.express_id)
            (output_dir, update_api) = GetUploadInfo(client, member_id, express_info['project_sn'], express_info['task_id'], 'gene_express_diff_stat')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "denovo_rna.report.diff_exp",
                "type": "workflow",
                "client": client,
                "project_sn": express_info["project_sn"],
                "to_file": ["denovo.export_express_matrix(express_file)", "denovo.export_control_file(control_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "express_file": data.express_id,
                    "update_info": update_info,
                    "group_file": data.group_id,
                    "group_detail": data.group_detail,
                    "control_file": data.control_id,
                    "control_detail": data.control_detail,
                    "ci": data.ci,
                    "rate": data.rate,
                    "express_id": str(express_id)
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
