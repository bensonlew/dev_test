# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController



class Hcluster(MetaController):
    def __init__(self):
        super(Hcluster, self).__init__()

    def POST(self):
        return_info = super(Hcluster, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'group_id', 'group_detail',
                        'distance_algorithm', 'hcluster_method', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.task_name = 'meta.report.hcluster'
        self.task_type = 'workflow'  # 可以不配置
        params_json = {
            'otu_id': data.otu_id,
            'level_id': data.level_id,
            'group_id': data.group_id,
            'group_detail': data.group_detail,
            'distance_algorithm': data.distance_algorithm,
            'hcluster_method': data.hcluster_method,
            'submit_location': data.submit_location,
            'task_type': data.task_type
            }
        self.options = {
            'otu_table': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'distance_algorithm': data.distance_algorithm,
            'hcluster_method': data.hcluster_method,
            'group_detail': data.group_detail,
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':'))
            }
        self.to_file = 'meta.export_otu_table_by_detail(otu_table)'
        self.run()
        return self.returnInfo
