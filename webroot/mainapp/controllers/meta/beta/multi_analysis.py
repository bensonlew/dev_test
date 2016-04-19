# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
from bson.errors import InvalidId
from mainapp.libs.param_pack import group_detail_sort
# import bson.errors.InvalidId
import random
import datetime
import time
import types


class MultiAnalysis(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        default_argu = ['analysis_type', 'otu_id', 'level_id', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        object_otu_id = self.check_objectid(data.otu_id)
        if object_otu_id:
            otu_info = Meta().get_otu_table_info(object_otu_id)
            if otu_info:
                params_json = {
                    'otu_id': data.otu_id,
                    'level_id': int(data.level_id),
                    'analysis_type': data.analysis_type,
                    'submit_location': data.submit_location
                }
                env_id = None
                env_labs = ''
                dist_method = ''
                group_id = None
                if data.analysis_type == 'pca':
                    if hasattr(data, 'env_id'):
                        params_json['env_id'] = data.env_id
                        env_id = self.check_objectid(data.env_id)
                        if not env_id:
                            info = {'success': False, 'info': 'env_id格式:%s不正确，无法转换为ObjectId格式！' % data.env_id}
                            return json.dumps(info)
                        if hasattr(data, 'env_labs'):
                            params_json['env_labs'] = data.env_labs
                            env_labs = data.env_labs
                        else:
                            info = {'success': False, 'info': '没有选择任何环境因子列'}
                            return json.dumps(info)
                elif data.analysis_type == 'pcoa' or data.analysis_type == 'nmds':
                    if not hasattr(data, 'distance_algorithm'):
                        info = {'success': False, 'info': 'distance_algorithm参数缺少!'}
                        return json.dumps(info)
                    params_json['distance_algorithm'] = data.distance_algorithm
                    dist_method = data.distance_algorithm
                elif data.analysis_type == 'dbrda':
                    if not hasattr(data, 'distance_algorithm'):
                        info = {'success': False, 'info': 'distance_algorithm参数缺少!'}
                        return json.dumps(info)
                    params_json['distance_algorithm'] = data.distance_algorithm
                    dist_method = data.distance_algorithm
                    if hasattr(data, 'env_id'):
                        params_json['env_id'] = data.env_id
                        env_id = self.check_objectid(data.env_id)
                        if not env_id:
                            info = {'success': False, 'info': 'group_id格式:%s不正确，无法转换为ObjectId格式！' % data.env_id}
                            return json.dumps(info)
                        if hasattr(data, 'env_labs'):
                            params_json['env_labs'] = data.env_labs
                            env_labs = data.env_labs
                        else:
                            info = {'success': False, 'info': '没有选择任何环境因子列'}
                            return json.dumps(info)
                    else:
                        info = {'success': False, 'info': 'dbrda分析缺少参数:env_id!'}
                        return json.dumps(info)
                elif data.analysis_type == 'rda_cca':
                    if hasattr(data, 'env_id'):
                        params_json['env_id'] = data.env_id
                        env_id = self.check_objectid(data.env_id)
                        if not env_id:
                            info = {'success': False, 'info': 'env_id格式:%s不正确，无法转换为ObjectId格式！' % data.env_id}
                            return json.dumps(info)
                        if hasattr(data, 'env_labs'):
                            params_json['env_labs'] = data.env_labs
                            env_labs = data.env_labs
                        else:
                            info = {'success': False, 'info': '没有选择任何环境因子列'}
                            return json.dumps(info)
                    else:
                        info = {'success': False, 'info': 'rda_cca分析缺少参数:env_id!'}
                        return json.dumps(info)
                elif data.analysis_type == 'plsda':
                    if hasattr(data, 'group_id'):
                        params_json['group_id'] = data.group_id
                        group_id = self.check_objectid(data.group_id)
                        if not group_id:
                            info = {'success': False, 'info': 'group_id格式:%s不正确，无法转换为ObjectId格式！' % data.group_id}
                            return json.dumps(info)
                        if hasattr(data, 'group_detail'):
                            try:
                                group = json.loads(data.group_detail)
                            except ValueError:
                                info = {'success': False, 'info': 'group_detail格式不正确!:%s' % data.group_detail}
                                return json.dumps(info)
                            params_json['group_detail'] = group_detail_sort(data.group_detail)
                            if len(group) < 2:
                                info = {'success': False, 'info': '不可只选择一个分组'}
                                return json.dumps(info)
                            samples = reduce(lambda x, y: x + y, group.values())
                            if len(samples) == len(set(samples)):
                                pass
                            else:
                                info = {'success': False, 'info': '不同分组存在相同的样本id'}
                                return json.dumps(info)
                        else:
                            info = {'success': False, 'info': '没有group_detail数据'}
                            return json.dumps(info)
                    else:
                        info = {'success': False, 'info': 'plsda分析缺少参数:group_id!'}
                        return json.dumps(info)
                else:
                    info = {'success': False, 'info': '不正确的分析方法:%s' % data.analysis_type}
                    return json.dumps(info)
                insert_mongo_json = {
                    'project_sn': otu_info['project_sn'],
                    'task_id': otu_info['task_id'],
                    'otu_id': object_otu_id,
                    'level_id': int(data.level_id),
                    'name': (data.analysis_type + '_' + otu_info['name'] +
                             '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
                    'table_type': data.analysis_type,
                    'env_id': env_id,
                    'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
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
                    'name': 'meta.report.beta_multi_analysis',
                    'type': 'workflow',
                    'client': client,
                    'project_sn': otu_info['project_sn'],
                    'to_file': ['meta.export_otu_table_by_level(otu_file)'],
                    'USE_DB': True,
                    'IMPORT_REPORT_DATA': True,
                    'UPDATE_STATUS_API': 'meta.update_status',
                    # 'UPDATE_STATUS_API': 'test',
                    'output': ("sanger:rerewrweset/" + str(otu_info["project_sn"]) + "/" +
                               str(otu_info['task_id']) + "/report_results/" + '%s_' % data.analysis_type +
                               str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))),
                    'options': {
                        'analysis_type': data.analysis_type,
                        'update_info': update_info,
                        'otu_file': data.otu_id,
                        'otu_id': data.otu_id,
                        'level': int(data.level_id),
                        'dist_method': dist_method,
                        'env_labs': env_labs,
                        'multi_analysis_id': str(multi_analysis_id)
                    }
                }
                if env_id:
                    json_data['to_file'].append('env.export_env_table(env_file)')
                    json_data['options']['env_file'] = data.env_id
                elif group_id:
                    json_data['to_file'].append('meta.export_group_table_by_detail(group_file)')
                    json_data['options']['group_file'] = data.group_id
                    json_data['options']['group_detail'] = data.group_detail
                insert_data = {'client': client,
                               'workflow_id': workflow_id,
                               'json': json.dumps(json_data, sort_keys=True, separators=(',', ':')),
                               'ip': web.ctx.ip
                               }
                workflow_module = Workflow()
                workflow_module.add_record(insert_data)
                info = {'success': True, 'info': '提交成功!', '_id': str(multi_analysis_id)}
                return json.dumps(info)
            else:
                info = {'success': False, 'info': 'OTU不存在，请确认参数是否正确！!'}
                return json.dumps(info)
        else:
            info = {'success': False, 'info': 'otu_id格式:%s不正确，无法转换为ObjectId格式！' % data.otu_id}
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
