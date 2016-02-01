# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import random
import datetime
import time


class DistanceCalc(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        otu_level = 9
        if not hasattr(data, "otu_id"):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        if hasattr(data, 'level_id'):
            otu_level = data.level_id
        method = 'bray_curtis'
        if hasattr(data, 'distance_algorithm'):
            method = data.distance_algorithm
        otu_info = Meta().get_otu_table_info(data.otu_id)
        params_json = {
            "otu_id": ObjectId(data.otu_id),
            "level_id": data.level_id,
            "distance_algorithm": method
        }
        if otu_info:
            insert_mongo_json = {
                'project_sn': otu_info['project_sn'],
                'task_id': otu_info['task_id'],
                'otu_id': ObjectId(data.otu_id),
                'level_id': otu_level,
                'name': method + '_' + otu_info['name'] + '_' + time.asctime(time.localtime(time.time())),
                'distance_algorithm': method,
                'params': json.dumps(params_json),
                'status': 'start',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            collection = get_mongo_client()['sanger']['sg_beta_specimen_distance']
            distance_matrix_id = collection.insert_one(insert_mongo_json).inserted_id
            update_info = {str(distance_matrix_id): "sg_beta_specimen_distance"}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.distance_calc",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": "meta.export_otu_table_by_level(otu_file)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": update_info,
                    "otu_file": data.otu_id,
                    "otu_id": data.otu_id,
                    "level": otu_level,
                    "method": method,
                    "matrix_id": str(distance_matrix_id)
                }
            }
            print json_data
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!", '_id': str(distance_matrix_id)}
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
