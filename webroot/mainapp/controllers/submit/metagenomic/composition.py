# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'

import web
import json
import datetime
from bson import ObjectId
from bson import SON
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.metagenomic import Metagenomic
from mainapp.controllers.project.metagenomic_controller import MetagenomicController


class CompositionAction(MetagenomicController):
    def __init__(self):
        super(CompositionAction, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        default_argu = ['anno_type', 'geneset_id', 'method', 'submit_location', 'group_id', 'group_detail',
                        'group_method', 'graphic_type']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {"success": False, "info": "缺少参数%s!" % argu}
                return json.dumps(info)
        if data.group_method not in ["", "sum", "average", "middle"]:
            info = {"success": False, "info": "对分组样本计算方式:%s错误!" % data.group_method}
            return json.dumps(info)
        if data.anno_type not in ["nr", "kegg", "cog", "vfdb", "ardb", "card", "cazy", "gene"]:
            info = {"success": False, "info": "数据库类型:%s错误!" % data.anno_type}
            return json.dumps(info)
        if data.anno_type not in ["gene"]:
            if not hasattr(data,  'anno_id'):
                info = {"success": False, "info": "缺少anno_id参数!"}
                return json.dumps(info)
            if not hasattr(data, 'level_id'):
                info = {"success": False, "info": "缺少level_id参数!"}
                return json.dumps(info)
        task_name = 'metagenomic.report.composition'
        task_type = 'workflow'
        metagenomic = Metagenomic()
        geneset_info = metagenomic.get_geneset_info(data.geneset_id)
        task_info = metagenomic.get_task_info(geneset_info['task_id'])
        params_json = {
            'anno_type': data.anno_type,
            'geneset_id': data.geneset_id,
            'method': data.method,
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'group_method': data.group_method,
            'graphic_type': data.graphic_type,
            'task_type': task_type,
            'submit_location': data.submit_location
        }
        group_id = data.group_id if data.group_id in ['all', 'All', 'ALL'] else ObjectId(data.group_id)
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('geneset_id', ObjectId(data.geneset_id)),
            ('status', 'start'),
            ('group_id', group_id),
            ('desc', '正在计算'),
            ('anno_type', data.anno_type),
            ('graphic_type', data.graphic_type),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        level = ''
        if data.anno_type not in ["gene"]:
            params_json['anno_id'] = data.anno_id
            params_json['level_id'] = data.level_id  # 此处是level缩写，需要再转义成真实level name
            level = data.level_id
            mongo_data.append(('anno_id', ObjectId(data.anno_id)))
            mongo_data.append(('level_id', data.level_id))
            if hasattr(data, 'second_level'):
                params_json['second_level'] = data.second_level
                mongo_data.append(('second_level', data.second_level))
        main_table_name = 'Community' + data.graphic_type.capitalize() + '_' + data.anno_type.upper() + '_' +level + '_' + \
            datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        mongo_data.append(('name', main_table_name))
        if data.graphic_type in ["bar", "circos"]:
            if hasattr(data, 'combine_value'):
                params_json['combine_value'] = data.combine_value
            else:
                info = {'success': False, 'info': '没有提供Others合并'}
                return json.dumps(info)
        if data.graphic_type in ["heatmap"]:
            if hasattr(data, 'top'):
                params_json['top'] = data.top
            else:
                info = {'success': False, 'info': '没有提供选择分类水平总丰度前'}
                return json.dumps(info)
            if hasattr(data, 'species_cluster'):
                params_json['species_cluster'] = data.species_cluster
            else:
                info = {'success': False, 'info': '没有提供物种聚类方式'}
                return json.dumps(info)
            if hasattr(data, 'specimen_cluster'):
                params_json['specimen_cluster'] = data.top
            else:
                info = {'success': False, 'info': '没有提供样品聚类方式'}
                return json.dumps(info)
        mongo_data.append(('params', json.dumps(params_json, sort_keys=True, separators=(',', ':'))))
        main_table_id = self.metagenomic.insert_main_table('composition', mongo_data)
        update_info = {str(main_table_id): 'composition'}
        options = {
            'graphic_type': data.graphic_type,
            'method': data.method,
            'group_method': data.group_method,
            'update_info': json.dumps(update_info),
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            'group_id': data.group_id,
            'geneset_id': data.geneset_id,
            'group_detail': data.group_detail,
            'geneset_table': data.geneset_id,
            'anno_type': data.anno_type,
            'group': data.group_id
        }
        to_file = ['metagenomic.export_geneset_table(geneset_table)']
        to_file.append('metagenomic.export_group_table_by_detail(group)')
        if data.anno_type not in ['gene']:
            options['level_id'] = data.level_id
            options['anno_id'] = data.anno_id
            options['anno_table'] = data.anno_id
            to_file.append('metagenomic.export_anno_table_path(anno_table)')
            if data.anno_type not in 'nr':
                options['lowest_level'] = data.anno_id
                to_file.append('metagenomic.export_anno_lowest_leve(lowest_level)')
            if hasattr(data, 'second_level'):
                options['second_level'] = data.second_level
        else:
            options['gene_list'] = data.geneset_id
            to_file.append('metagenomic.export_geneset_list(gene_list)')
        options['main_id'] = str(main_table_id)
        options['main_table_data'] = SON(mongo_data)
        self.set_sheet_data(name=task_name, options=options,
                            main_table_name=main_table_name, task_id=task_info['task_id'],
                            project_sn=task_info['project_sn'],module_type=task_type, params=params_json, to_file=to_file)
        task_info = super(CompositionAction, self).POST()
        if task_info['success']:
            task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        return json.dumps(task_info)
