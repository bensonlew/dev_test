# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
import datetime
from bson import ObjectId
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.meta import Meta
from mainapp.libs.param_pack import group_detail_sort


class PlotTree(MetaController):
    def __init__(self):
        super(PlotTree, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'color_level_id', 'group_id', 'group_detail', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        if int(data.level_id) < int(data.color_level_id):
            info = {'success': False, 'info': '颜色设置水平必须高于进化树绘制水平!'}
            return json.dumps(info)
        task_name = 'meta.report.plot_tree'
        task_type = 'workflow'  # 可以不配置
        meta = Meta()
        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        params = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'color_level_id': int(data.color_level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        main_table_name = 'PlotTree_' + \
            '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('status', 'start'),
            ('desc', '正在计算'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("params", params)
        ]
        main_table_id = meta.insert_main_table('sg_phylo_tree', mongo_data)
        update_info = {str(main_table_id): 'sg_phylo_tree'}
        options = {'otu_table': data.otu_id,
                   'otu_id': data.otu_id,
                   'level': int(data.level_id),
                   'color_level_id': int(data.color_level_id),
                   'sample_group': data.group_id,
                   'group_id': data.group_id,
                   'update_info': json.dumps(update_info),
                   'group_detail': data.group_detail,
                   'params': params,
                   'main_id': str(main_table_id)
                   }
        to_file = ['meta.export_otu_table_by_detail(otu_table)', 'meta.export_group_table_by_detail(sample_group)']
        self.set_sheet_data(name=task_name,
                            options=options,
                            main_table_name=main_table_name,
                            module_type=task_type,
                            to_file=to_file)
        task_info = super(PlotTree, self).POST()
        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        return json.dumps(task_info)
