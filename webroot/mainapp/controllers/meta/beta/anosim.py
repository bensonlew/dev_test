# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
import random
import datetime
import time
import types
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
from bson.errors import InvalidId
from mainapp.libs.param_pack import group_detail_sort


class Anosim(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        default_argu = ['otu_id', 'level_id', 'distance_algorithm', 'permutations', 'group_id', 'group_detail', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        try:
            group = json.loads(data.group_detail)
            print data.group_detail
        except ValueError:
            info = {'success': False, 'info': 'group_detail格式不正确!:%s' % data.group_detail}
            return json.dumps(info)
        try:
            int(data.permutations)
        except ValueError:
            info = {'success': False, 'info': 'permutations格式应该为数字!:%s' % data.permutations}
            return json.dumps(info)
        if len(group) < 2:
            info = {'success': False, 'info': '不可只选择一个分组'}
            return json.dumps(info)
        samples = reduce(lambda x, y: x + y, group.values())
        if len(samples) == len(set(samples)):
            pass
        else:
            info = {'success': False, 'info': '不同分组存在相同的样本id'}
            return json.dumps(info)
        if len(samples) <= len(group):
            info = {'success': False, 'info': '不可每个组都只含有一个样本'}
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
            'permutations': data.permutations,
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location
        }

        if otu_info:
            insert_mongo_json = {
                'project_sn': otu_info['project_sn'],
                'task_id': otu_info['task_id'],
                'otu_id': ObjectId(data.otu_id),
                'level_id': int(data.level_id),
                'name': ('anosim_group_' + otu_info['name'] +
                         '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
                'group_id': data.group_id,
                'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
                'status': 'start',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            collection = get_mongo_client()['sanger']['sg_beta_multi_anosim']
            anosim_id = collection.insert_one(insert_mongo_json).inserted_id
            update_info = {str(anosim_id): 'sg_beta_multi_anosim'}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(otu_info['task_id'], data.otu_id)
            json_data = {
                'id': workflow_id,
                'stage_id': 0,
                'name': 'meta.report.anosim',
                'type': 'workflow',
                'client': client,
                'project_sn': otu_info['project_sn'],
                'to_file': ['meta.export_otu_table_by_level(otu_file)',
                            'meta.export_group_table_by_detail(group_file)'],
                'USE_DB': True,
                'IMPORT_REPORT_DATA': True,
                'UPDATE_STATUS_API': 'meta.update_status',
                # 'UPDATE_STATUS_API': 'test',
                'output': ("sanger:rerewrweset/" + str(otu_info["project_sn"]) + "/" +
                           str(otu_info['task_id']) + "/report_results/" + 'anosim_' +
                           str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))),
                'options': {
                    'update_info': update_info,
                    'otu_file': data.otu_id,
                    'otu_id': data.otu_id,
                    'level': int(data.level_id),
                    'method': data.distance_algorithm,
                    'anosim_id': str(anosim_id),
                    'group_file': data.group_id,
                    'group_id': data.group_id,
                    'group_detail': data.group_detail,
                    'samples': ','.join(samples),
                    'permutations': int(data.permutations)
                }
            }
            insert_data = {'client': client,
                           'workflow_id': workflow_id,
                           'json': json.dumps(json_data, sort_keys=True, separators=(',', ':')),
                           'ip': web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {'success': True, 'info': '提交成功!', '_id': str(anosim_id)}
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
