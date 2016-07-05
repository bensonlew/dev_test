# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from bson.objectid import ObjectId
from bson.errors import InvalidId
from mainapp.libs.param_pack import group_detail_sort
# import bson.errors.InvalidId
import types
from mainapp.controllers.project.meta_controller import MetaController


class MultiAnalysis(MetaController):
    def __init__(self):
        super(MultiAnalysis, self).__init__()

    def POST(self):
        return_info = super(MultiAnalysis, self).POST()
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['analysis_type', 'otu_id', 'level_id', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.task_name = 'meta.report.beta_multi_analysis'
        self.task_type = 'workflow'  # 可以不配置
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
        self.options = {
            'analysis_type': data.analysis_type,
            'otu_file': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'dist_method': dist_method,
            'env_labs': env_labs,
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            }
        if env_id:
            self.to_file.append('env.export_env_table(env_file)')
            self.options['env_file'] = data.env_id
            self.options['env_id'] = data.env_id
        elif group_id:
            self.to_file.append('meta.export_group_table_by_detail(group_file)')
            self.options['group_file'] = data.group_id
            self.options['group_id'] = data.group_id
            self.options['group_detail'] = data.group_detail
        self.run()
        return self.returnInfo

    def check_objectid(self, in_id):
        """检查一个id是否可以被ObjectId"""
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
