# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.distance_matrix import Distance
from mainapp.config.db import get_mongo_client
from bson.errors import InvalidId
from bson.objectid import ObjectId
import random
import datetime
import time
import types


class Hcluster(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        default_argu = ['specimen_distance_id', 'hcluster_method', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        object_matrix_id = self.check_objectid(data.specimen_distance_id)
        if object_matrix_id:
            matrix_info = Distance().get_distance_matrix_info(object_matrix_id)
        else:
            info = {'success': False, 'info': 'specimen_distance_id格式不正确!'}
            return json.dumps(info)
        params_json = {
            'specimen_distance_id': data.specimen_distance_id,
            'hcluster_method': data.hcluster_method,
            'submit_location': data.submit_location
        }
        if matrix_info:
            task_info = Meta().get_task_info(matrix_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(matrix_info["task_id"])}
                return json.dumps(info)
            insert_mongo_json = {
                'task_id': matrix_info['task_id'],
                'table_id': object_matrix_id,
                'table_type': 'dist',
                'name': 'hcluster_' + data.hcluster_method + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
                'tree_type': 'cluster',
                'hcluster_method': data.hcluster_method,
                'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
                'status': 'start',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            collection = get_mongo_client()['sanger']['sg_newick_tree']
            newicktree_id = collection.insert_one(insert_mongo_json).inserted_id
            update_info = {str(newicktree_id): 'sg_newick_tree'}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(matrix_info['task_id'], data.specimen_distance_id)
            json_data = {
                'id': workflow_id,
                'stage_id': 0,
                'name': 'meta.report.hcluster',
                'type': 'workflow',
                'client': client,
                'project_sn': matrix_info['project_sn'],
                'to_file': 'dist_matrix.export_distance_matrix(distance_matrix)',
                'USE_DB': True,
                'IMPORT_REPORT_DATA': True,
                'UPDATE_STATUS_API': 'meta.update_status',
                # 'UPDATE_STATUS_API': 'test',
                'output': ("sanger:rerewrweset/files/" + str(member_id) + '/' + str(matrix_info["project_sn"]) + "/" +
                           str(matrix_info['task_id']) + "/report_results/" + 'hcluster_' +
                           str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))),
                'options': {
                    'update_info': update_info,
                    'distance_matrix': data.specimen_distance_id,
                    'distance_id': data.specimen_distance_id,
                    'method': data.hcluster_method,
                    'newick_id': str(newicktree_id)
                }
            }
            insert_data = {'client': client,
                           'workflow_id': workflow_id,
                           'json': json.dumps(json_data, sort_keys=True, separators=(',', ':')),
                           'ip': web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {'success': True, 'info': '提交成功!', '_id': str(newicktree_id)}
            return json.dumps(info)
        else:
            info = {'success': False, 'info': '距离矩阵不存在，请确认参数是否正确！!'}
            return json.dumps(info)

    def get_new_id(self, task_id, distance_id):
        new_id = '%s_%s_%s' % (task_id, distance_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, distance_id)
        return new_id


    def check_objectid(self, in_id):
        '''
        检查一个id是否可以被ObjectId
        '''
        if isinstance(in_id, types.StringTypes):
            try:
                in_id = ObjectId(in_id)
            except InvalidId:
                return False
        elif isinstance(in_id, ObjectId):
            pass
        else:
            return False
        return in_id
