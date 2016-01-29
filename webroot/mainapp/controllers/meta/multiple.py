# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

import web
import json
import random
import datetime
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.group_stat import GroupStat as G


class Multiple(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "otu_id")):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level
        my_param['group_detail'] = data.group_detail
        my_param['group_id'] = data.group_id
        my_param['correction'] = data.correction
        my_param['test'] = data.test  
        params = json.dumps(my_param)
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + "_multiple_stat_table"
            multiple_id = G().create_species_difference_check(data.level, 'multiple', params, data.group_id,
                                                              data.otu_id, name)
            update_info = {str(multiple_id): "sg_species_difference_check"}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.multiple",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": ["meta.export_otu_table_by_level(otu_file)", "meta.export_group_table_by_detail(group_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.multiple",
                "options": {
                    "update_info": update_info,
                    "otu_file": data.otu_id,
                    "level": data.level,
                    "group_file": data.group_detail,
                    "correction": data.correction,
                    "test": data.test,
                    "multiple_id": str(multiple_id)
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
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

