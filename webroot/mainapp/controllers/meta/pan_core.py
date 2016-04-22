# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import random
import datetime
import re
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.pan_core import PanCore as P
from mainapp.libs.param_pack import param_pack


class PanCore(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        param_list = ["group_id", "category_name", "otu_id", "level_id", "submit_location"]
        for my_p in param_list:
            if not hasattr(data, my_p):
                info = {"success": False, "info": "缺少参数{}!".format(my_p)}
                return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        c_name = re.split(',', data.category_name)
        c_name.sort()
        new_cname = ','.join(c_name)
        my_param['category_name'] = new_cname
        my_param["submit_location"] = data.submit_location
        params = param_pack(my_param)
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "pan_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            pan_id = P().create_pan_core_table(1, params, data.group_id, data.level_id, data.otu_id, name)
            name = "core_table" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            core_id = P().create_pan_core_table(2, params, data.group_id, data.level_id, data.otu_id, name)
            update_info = {str(pan_id): "sg_otu_pan_core", str(core_id): "sg_otu_pan_core"}
            # 字典  id: 表名
            update_info = json.dumps(update_info)
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            pre_path = "sanger:rerewrweset/" + str(member_id) + "/" + str(otu_info["project_sn"]) + "/" + str(otu_info['task_id']) + "/report_results/"
            suff_path = "pan_core_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            output_dir = pre_path + suff_path

            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.pan_core",  # src/mbio/meta/report/pan_core
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": ["meta.export_otu_table_by_level(in_otu_table)", "meta.export_group_table(group_table)"],
                # src/mbio/api/to_file/meta 括号内的值与options里面的值对应
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",  # src/mbio/api/web/update_status
                "output": output_dir,
                "options": {
                    "update_info": update_info,
                    "in_otu_table": data.otu_id,
                    "group_table": data.group_id,
                    "category_name": data.category_name,
                    "level": data.level_id,
                    "pan_id": str(pan_id),
                    "core_id": str(core_id)
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!正在生成pan otu表和core otu表..."}
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
