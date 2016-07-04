# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
# from collections import OrderedDict
import random
import datetime
# import time
import types
from bson.errors import InvalidId
from biocluster.wsheet import Sheet
# from biocluster import workflow
from mbio.workflows.single import SingleWorkflow
from mbio.workflows.meta.report.distance_calc import DistanceCalcWorkflow


class DistanceCalc(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        default_argu = ['otu_id', 'level_id', 'distance_algorithm', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        object_otu_id = self.check_objectid(data.otu_id)
        if object_otu_id:
            otu_info = Meta().get_otu_table_info(data.otu_id)
        else:
            info = {'success': False, 'info': 'otu_id格式不正确!'}
            return json.dumps(info)
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'distance_algorithm': data.distance_algorithm,
            'submit_location': data.submit_location
            }
        if otu_info:
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            insert_mongo_json = {
                'project_sn': otu_info['project_sn'],
                'task_id': otu_info['task_id'],
                'otu_id': ObjectId(data.otu_id),
                'level_id': int(data.level_id),
                'name': (data.distance_algorithm + '_' + otu_info['name'] +
                         '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
                'distance_algorithm': data.distance_algorithm,
                'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
                'status': 'start',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            collection = get_mongo_client()['sanger']['sg_beta_specimen_distance']
            distance_matrix_id = collection.insert_one(insert_mongo_json).inserted_id
            update_info = {str(distance_matrix_id): 'sg_beta_specimen_distance'}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(otu_info['task_id'], data.otu_id)
            json_data = {
                'id': workflow_id,
                'stage_id': 0,
                'name': 'meta.report.distance_calc',
                'type': 'workflow',
                'client': client,
                'project_sn': otu_info['project_sn'],
                'to_file': 'meta.export_otu_table_by_level(otu_file)',
                'USE_DB': True,
                'IMPORT_REPORT_DATA': True,
                # 'UPDATE_STATUS_API': 'test',
                'output': ("sanger:rerewrweset/files/" + str(member_id) + '/' + str(otu_info["project_sn"]) + "/" +
                           str(otu_info['task_id']) + "/report_results/" + 'distance_calc_' +
                           str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))),
                'UPDATE_STATUS_API': 'meta.update_status',
                'options': {
                    'update_info': update_info,
                    'otu_file': data.otu_id,
                    'otu_id': data.otu_id,
                    'level': int(data.level_id),
                    'method': data.distance_algorithm,
                    'matrix_id': str(distance_matrix_id)
                    }
                }
            # insert_data = {'client': client,
            #                'workflow_id': workflow_id,
            #                'json': json.dumps(json_data, sort_keys=True, separators=(',', ':')),
            #                'ip': web.ctx.ip
            #                }
            # workflow_module = Workflow()
            # workflow_module.add_record(insert_data)
            mySheet = Sheet(data=json_data)
            myWorkflow = SingleWorkflow(mySheet)
            myWorkflow.run()
            info = {'success': True, 'info': '提交成功!', '_id': str(distance_matrix_id)}
            return json.dumps(info)
        else:
            info = {'success': False, 'info': 'OTU不存在，请确认参数是否正确！!'}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = '%s_%s_%s' % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

    def check_objectid(self, in_id):
        """
        检查一个id是否可以被ObjectId
        """
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
