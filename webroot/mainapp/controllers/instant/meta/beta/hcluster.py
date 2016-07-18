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
        default_argu = ['specimen_distance_id', 'hcluster_method', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.task_name = 'meta.report.hcluster'
        self.task_type = 'workflow'  # 可以不配置
        self.options = {
            'distance_matrix': data.specimen_distance_id,
            'distance_id': data.specimen_distance_id,
            'method': data.hcluster_method,
            'submit_location': data.submit_location,
            'task_type': data.taskType
            }
        self.to_file = 'dist_matrix.export_distance_matrix(distance_matrix)'
        self.run()
        return self.returnInfo
