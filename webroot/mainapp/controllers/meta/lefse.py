# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.group_stat import GroupStat as G


class Lefse(object):
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        self.check_options(data)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['group_detail'] = data.group_detail
        my_param['group_id'] = data.group_id
        my_param['lda_filter'] = data.lda_filter
        my_param['strict'] = data.strict  
        params = json.dumps(my_param)
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + "_lefse_lda_table"
            lefse_id = G().create_species_difference_lefse(params, data.group_id, data.otu_id, name)
            update_info = {str(lefse_id): "sg_species_difference_lefse"}
            update_info = json.dumps(update_info)

            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.lefse",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": ["meta.export_otu_table(otu_file)", "meta.export_group_table_by_detail(group_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "otu_file": data.otu_id,
                    "update_info": update_info,
                    "group_file": data.group_id,
                    "group_detail": data.group_detail,
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
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'group_detail', 'group_id', 'lda_filter', 'strict']
        for names in params_name:
            if not (hasattr(data, names)):
                info = {"success": False, "info": "缺少参数!"}
                return json.dumps(info)
        if int(data.strict) not in [1, 0]:
            info = {"success": False, "info": "严格性比较策略不在范围内"}
            return json.dumps(info)
        if float(data.lda_filter) > 4.0 or float(data.lda_filter) < -4.0:
            info = {"success": False, "info": "LDA阈值不在范围内"}
            return json.dumps(info)
