# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from bson import ObjectId


class HierarchicalClusteringHeatmap(MetaController):
    def __init__(self):  # 20170106 2 lines
        super(HierarchicalClusteringHeatmap, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        postArgs = ["otu_id", "level_id", "group_id", "group_detail", "species_number", "method", "task_type", "sample_method", "add_Algorithm"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        task_name = 'meta.report.hierarchical_clustering_heatmap'
        otu_info = self.meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        if data.method not in ["average", "single", "complete", ""]:
            info = {'success': False, "info": "参数method的值为{}，应该为average，single或者complete".format(data.method)}
        task_info = self.meta.get_task_info(otu_info['task_id'])
        params_json = {
            'submit_location': data.submit_location,
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_detail': group_detail_sort(data.group_detail),
            'group_id': data.group_id,
            'task_type': data.task_type,
            'species_number': data.species_number,
            "method": data.method,
            "sample_method": data.sample_method,
            "add_Algorithm": data.add_Algorithm
        }
        main_table_name = 'CommunityHeatmap_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),  # maybe ObjectId(data.otu_id)
            ('name', main_table_name),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':'))),
            ('status', 'start'),
            ('desc', '正在计算'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("show", 0),
            ("type", "otu_HierarchicalClusteringHeatmap")
        ]
        main_table_id = self.meta.insert_main_table('sg_hc_heatmap', mongo_data)
        update_info = {str(main_table_id): 'sg_hc_heatmap'}
        options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "level": str(data.level_id),
            'update_info': json.dumps(update_info),
            'main_id': str(main_table_id),
            "species_number": data.species_number,  # 筛选物种参数
            "method": data.method,
            "sample_method": data.sample_method,  # 样本聚类方式
            "add_Algorithm": data.add_Algorithm
        }
        to_file = "meta.export_otu_table_by_level(in_otu_table)"
        self.set_sheet_data(name=task_name, options=options, main_table_name="CommunityAnalysis/" + main_table_name,
                            module_type='workflow', to_file=to_file)
        task_info = super(HierarchicalClusteringHeatmap, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }}
        return json.dumps(task_info)
