# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.distance_matrix import Distance
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import random
import datetime


class Hcluster(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "distance_id") and hasattr(data, "name")):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        matrix_info = Distance().get_distance_matrix_info(data.distance_id)
        method = 'average'
        if hasattr(data, 'method'):
            method = data.method
        insert_mongo_json = {
            'task_id': matrix_info["task_id"],
            'table_id': data.distance_id,
            'table_type': 'dist',
            'name': data.name,
            'tree_Type': 'cluster',
            'status': 'start',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = get_mongo_client()['sanger']['sg_newick_tree']
        newicktree_id = collection.insert_one(insert_mongo_json).inserted_id
        if matrix_info:
            workflow_id = self.get_new_id(matrix_info["task_id"], data.distance_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.Hcluster",
                "type": "workflow",
                "client": client,
                "project_sn": matrix_info["project_sn"],
                "to_file": "meta.export_distance_matrix_table(distance_matrix)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "distance_matrix": data.distance_id,
                    "name": data.name,
                    "distance_id": data.distance_id,
                    "task_id": matrix_info["task_id"],
                    "methd": method,
                    "newick_id": newicktree_id
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!", '_id': newicktree_id}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, distance_id):
        new_id = "%s_%s_%s" % (task_id, distance_id[-4:], random.randint(1, 100))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, distance_id)
        return new_id
