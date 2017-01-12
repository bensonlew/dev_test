# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
import datetime
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.meta import Meta
from bson import ObjectId


class Hcluster(MetaController):

    def __init__(self):
        super(Hcluster, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'group_id', 'group_detail',
                        'distance_algorithm', 'hcluster_method', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        task_name = 'meta.report.hcluster'
        task_type = 'workflow'
        meta = Meta()
        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'distance_algorithm': data.distance_algorithm,
            'hcluster_method': data.hcluster_method,
            'submit_location': data.submit_location,
            'task_type': data.task_type
        }
        main_table_name = 'Hcluster_' + data.hcluster_method + \
            '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('table_id', ObjectId(data.otu_id)),
            ('table_type', 'dist'),
            ('tree_type', 'cluster'),
            ('status', 'start'),
            ('desc', '正在计算'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("level_id", int(data.level_id)),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = meta.insert_main_table('sg_newick_tree', mongo_data)
        update_info = {str(main_table_id): 'sg_newick_tree'}
        options = {
            'otu_table': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'dist_method': data.distance_algorithm,
            'hcluster_method': data.hcluster_method,
            'group_detail': data.group_detail,
            'update_info': json.dumps(update_info),
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            'main_id': str(main_table_id)
        }
        to_file = 'meta.export_otu_table_by_detail(otu_table)'
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(Hcluster, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
                }}
        return json.dumps(task_info)
        # return json.dumps({'success': True, 'info': 'shenghe log'})
