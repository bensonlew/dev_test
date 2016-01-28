# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.estimator import Estimator
import random
import datetime


class Estimators(object):
    """

    """
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "otu_id") and hasattr(data, "name")):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level'] = data.level
        my_param['indices'] = data.index_type
        params = json.dumps(my_param)

        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + "_estimators"
            est_id = Estimator().add_est_collection(data.level, params, data.otu_id, name)
            update_info = {str(est_id): "sg_alpha_diversity"}
            update_info = json.dumps(update_info)

            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.estimators",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": "meta.export_otu_table_by_level(otu_table)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": update_info,
                    "otu_id": data.otu_id,
                    "otu_table": data.otu_id,
                    "task_id": otu_info["task_id"],
                    "indices": data.index_type,
                    "level": data.level,
                    "est_id": str(est_id)
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            # return json.dumps(json_obj)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 100))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
