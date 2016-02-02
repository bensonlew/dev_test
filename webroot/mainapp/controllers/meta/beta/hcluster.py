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
import time


class Hcluster(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "specimen_distance_id"):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        matrix_info = Distance().get_distance_matrix_info(data.specimen_distance_id)
        method = 'average'
        if hasattr(data, 'method'):
            method = data.method
        params_json = {
            "specimen_distance_id": data.specimen_distance_id,
            "method": method
        }
        if matrix_info:
            insert_mongo_json = {
                'task_id': matrix_info["task_id"],
                'table_id': ObjectId(data.specimen_distance_id),
                'table_type': 'dist',
                'name': 'hcluster_' + method + '_' + time.asctime(time.localtime(time.time())),
                'tree_type': 'cluster',
                'hcluster_method': method,
                'params': json.dumps(params_json),
                'status': 'start',
                'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            collection = get_mongo_client()['sanger']['sg_newick_tree']
            newicktree_id = collection.insert_one(insert_mongo_json).inserted_id
            update_info = {str(newicktree_id): "sg_newick_tree"}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(matrix_info["task_id"], data.specimen_distance_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.hcluster",
                "type": "workflow",
                "client": client,
                "project_sn": matrix_info["project_sn"],
                "to_file": "dist_matrix.export_distance_matrix(distance_matrix)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": update_info,
                    "distance_matrix": data.specimen_distance_id,
                    "distance_id": data.specimen_distance_id,
                    "method": method,
                    "newick_id": str(newicktree_id)
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!", '_id': str(newicktree_id)}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, distance_id):
        new_id = "%s_%s_%s" % (task_id, distance_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, distance_id)
        return new_id
