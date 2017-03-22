# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from bson.objectid import ObjectId
from bson.errors import InvalidId
from mainapp.libs.param_pack import group_detail_sort
# import bson.errors.InvalidId
import types
from mainapp.models.mongo.meta import Meta
import datetime
from mainapp.controllers.project.meta_controller import MetaController


class MultiAnalysis(MetaController):
    def __init__(self):
        super(MultiAnalysis, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['analysis_type', 'otu_id', 'level_id', 'submit_location', 'group_id', 'group_detail']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        task_name = 'meta.report.beta_multi_analysis'
        task_type = 'workflow'
        meta = Meta()
        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        group_detail = group_detail_sort(data.group_detail)
        main_table_name = MultiAnalysis.get_main_table_name(data.analysis_type)
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'analysis_type': data.analysis_type,
            'submit_location': data.submit_location,
            'task_type': data.task_type,
            'group_id': data.group_id,
            'group_detail': group_detail
            }
        env_id = None
        env_labs = ''
        dist_method = ''
        group_id = data.group_id if data.group_id in ['all', 'All', 'ALL'] else ObjectId(data.group_id)
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('table_type', data.analysis_type),
            ('status', 'start'),
            ('group_id', group_id),
            ('desc', '正在计算'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("level_id", int(data.level_id))
        ]
        sample_len = sum([len(i) for i in group_detail.values()])
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
            if sample_len < 3:
                info = {'success': False, 'info': '样本数量少于3，不可进行此分析！'}
                return json.dumps(info)
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
                    info = {'success': False, 'info': 'env_id格式:%s不正确，无法转换为ObjectId格式！' % data.env_id}
                    return json.dumps(info)
                if hasattr(data, 'env_labs'):
                    params_json['env_labs'] = data.env_labs
                    env_labs = data.env_labs
                else:
                    info = {'success': False, 'info': '没有选择任何环境因子列'}
                    return json.dumps(info)
            else:
                # info = {'success': False, 'info': 'dbrda分析缺少参数:env_id!'}
                info = {'success': False, 'info': 'dbrda分析缺少环境因子参数!'}  #modified by hongdongxuan 20170310
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
                # info = {'success': False, 'info': 'rda_cca分析缺少参数:env_id!'}
                info = {'success': False, 'info': 'rda_cca分析缺少环境因子参数!'}  #modified by hongdongxuan 20170310
                return json.dumps(info)
        elif data.analysis_type == 'plsda':
            try:
                group = json.loads(data.group_detail)
            except ValueError:
                info = {'success': False, 'info': 'group_detail格式不正确!:%s' % data.group_detail}
                return json.dumps(info)
            # params_json['group_detail'] = group_detail_sort(data.group_detail)
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
            info = {'success': False, 'info': '不正确的分析方法:%s' % data.analysis_type}
            return json.dumps(info)
        options = {
            'analysis_type': data.analysis_type,
            'otu_file': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'dist_method': dist_method,
            'env_labs': env_labs,
            'group_id': data.group_id,
            'group_detail': data.group_detail,
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            }
        to_file = ['meta.export_otu_table_by_detail(otu_file)']
        mongo_data.append(('env_id', env_id))
        if env_id:
            mongo_data.append(('env_labs', data.env_labs))
            to_file.append('env.export_env_table(env_file)')
            options['env_file'] = data.env_id
            options['env_id'] = data.env_id
        mongo_data.append(('params', json.dumps(params_json, sort_keys=True, separators=(',', ':'))))
        main_table_id = meta.insert_main_table('sg_beta_multi_analysis', mongo_data)
        update_info = {str(main_table_id): 'sg_beta_multi_analysis'}
        options['update_info'] = json.dumps(update_info)
        options['main_id'] = str(main_table_id)
        if data.analysis_type == 'plsda':
            to_file.append('meta.export_group_table_by_detail(group_file)')
            options['group_file'] = data.group_id
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(MultiAnalysis, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
                }}
        return json.dumps(task_info)

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

    @staticmethod
    def get_main_table_name(analysis_type):
        time_now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        if analysis_type == 'pca':
            return 'PCA_' + time_now
        elif analysis_type == 'pcoa':
            return 'PCoA_' + time_now
        elif analysis_type == 'nmds':
            return 'NMDS_' + time_now
        elif analysis_type == 'plsda':
            return 'PLS-DA_' + time_now
        elif analysis_type == 'dbrda':
            return 'db-RDA_' + time_now
        elif analysis_type == 'rda_cca':
            return 'RDACCA_' + time_now
        else:
            raise Exception('错误的分析类型')
