# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
import random
import time
import datetime


class Subsample(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "otu_id"):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        input_otu_info = Meta().get_otu_table_info(data.otu_id)
        if input_otu_info:
            output_otu_json = {
                'project_sn': input_otu_info['project_sn'],
                'task_id': input_otu_info['task_id'],
                'from_id': data.otu_id,
                'name': input_otu_info['name'] + '_' + time.asctime(time.localtime(time.time())),
                'params': json.dumps(data),
                'status': 'start',
                'desc': 'otu table after Subsample',
                'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            collection = get_mongo_client()['sanger']['sg_otu']
            output_otu_id = collection.insert_one(output_otu_json).inserted_id
            workflow_id = self.get_new_id(input_otu_info["task_id"], data.otu_id)
            workflow_json = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.otu_subsample",
                "type": "workflow",
                "client": client,
                "project_sn": input_otu_info["project_sn"],
                # "to_file": "meta.export_otu_table(otu_file)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": json.dumps({str(output_otu_id): "sg_otu"}),
                    "input_otu_id": data.otu_id,
                    "size": data.size if hasattr(data, "size") else 0,
                    "level": data.level_id if hasattr(data, "level_id") else 9,
                    "output_otu_id": str(output_otu_id)
                }
            }
            insert_mysql_data = {
                "client": client,
                "workflow_id": workflow_id,
                "json": json.dumps(workflow_json),
                "ip": web.ctx.ip
               }
            workflow_module = Workflow()
            workflow_module.add_record(insert_mysql_data)
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
