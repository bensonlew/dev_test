# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import bson.errors.InvalidId
import random
import datetime
import time


class MultiAnalysis(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, 'otu_id'):
            info = {'success': False, 'info': '缺少参数:otu_id!'}
            return json.dumps(info)
        otu_level = 9
        if hasattr(data, 'level_id'):
                otu_level = data.level_id
        if not hasattr(data, 'analysis_type'):
            info = {'success': False, 'info': '缺少参数:analysis_type!'}
            return json.dumps(info)
        else:
            try:
                bson_otu_id = ObjectId(data.otu_id)
            except bson.errors.InvalidId:
                info = {'success': False, 'info': 'otu_id格式:%s不正确，无法转换为ObjectId格式！' % data.otu_id}
                return json.dumps(info)
            otu_info = Meta().get_otu_table_info(bson_otu_id)
            if otu_info:
                params_json = {
                    'otu_id': data.otu_id,
                    'level_id': otu_level,
                    'analysis_type': data.analysis_type,
                }
                env_id = ''
                method = 'bray_curtis'
                if data.analysis_type == 'pca':
                    if hasattr(data, 'env_id'):
                        params_json['env_id'] = data.env_id
                        env_id = data.env_id
                if data.analysis_type == 'pcoa' or data.analysis_type == 'nmds':
                    if hasattr(data, 'distance_algorithm'):
                        method = data.distance_algorithm
                    params_json['distance_algorithm'] = method
                if data.analysis_type == 'dbrda':
                    if hasattr(data, 'distance_algorithm'):
                        method = data.distance_algorithm
                    params_json['distance_algorithm'] = method
                    if hasattr(data, 'group_id'):
                        params_json['group_id'] = data.group_id
                    else:
                        info = {'success': False, 'info': 'dbrda分析缺少参数:group_id!'}
                        return json.dumps(info)
                if data.analysis_type == 'rda_cca':
                    if hasattr(data, 'env_id'):
                        params_json['env_id'] = data.env_id
                        env_id = data.env_id
                    else:
                        info = {'success': False, 'info': 'rda_cca分析缺少参数:env_id!'}
                        return json.dumps(info)
                insert_mongo_json = {
                    'project_sn': otu_info['project_sn'],
                    'task_id': otu_info['task_id'],
                    'otu_id': ObjectId(data.otu_id),
                    'level_id': otu_level,
                    'name': (data.analysis_type + '_' + otu_info['name'] +
                             '_' + time.asctime(time.localtime(time.time()))),
                    'table_type': data.analysis_type,
                    'env_id': env_id,
                    'params': json.dumps(params_json),
                    'status': 'start',
                    'desc': '',
                    'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                collection = get_mongo_client()['sanger']['sg_beta_multi_analysis']
                multi_analysis_id = collection.insert_one(insert_mongo_json).inserted_id
                update_info = {str(multi_analysis_id): 'sg_beta_multi_analysis'}
                update_info = json.dumps(update_info)
                workflow_id = self.get_new_id(otu_info['task_id'], data.otu_id)
                json_data = {
                    'id': workflow_id,
                    'stage_id': 0,
                    'name': 'meta.report.multi_analysis',
                    'type': 'workflow',
                    'client': client,
                    'project_sn': otu_info['project_sn'],
                    'to_file': ['meta.export_otu_table_by_level(otu_file)'],
                    'USE_DB': True,
                    'IMPORT_REPORT_DATA': True,
                    'UPDATE_STATUS_API': 'meta.update_status',
                    'options': {
                        'analysis_type': data.analysis_type,
                        'update_info': update_info,
                        'otu_file': data.otu_id,
                        'otu_id': data.otu_id,
                        'level': otu_level,
                        'multi_analysis_id': str(multi_analysis_id)
                    }
                }
                if data.analysis_type == 'dbrda':
                    json_data['to_file'].append('meta.export_group_table_by_detail(group_file)')
                    json_data['option']['group_file'] = data.group_id
                if env_id:
                    json_data['to_file'].append('meta.export_env_table(env_file)')
                    json_data['option']['env_file'] = data.env_id
                insert_data = {'client': client,
                               'workflow_id': workflow_id,
                               'json': json.dumps(json_data),
                               'ip': web.ctx.ip
                               }
                # workflow_module = Workflow()
                # workflow_module.add_record(insert_data)
                print insert_data
                info = {'success': True, 'info': '提交成功!', '_id': str(multi_analysis_id)}
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
