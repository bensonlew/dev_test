# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController


class DistanceCalc(MetaController):
    def __init__(self):
        super(DistanceCalc, self).__init__()

    def POST(self):  # 建议不要在controller中写mongo的主表初始化表，统一在api.database中写入
        return_info = super(DistanceCalc, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'distance_algorithm', 'submit_location']  # 可以不要otu_id
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.task_name = 'meta.report.distance_calc'
        self.task_type = 'workflow'  # 可以不配置
        self.options = {'otu_file': data.otu_id,
                        'otu_id': data.otu_id,
                        'level': int(data.level_id),
                        'method': data.distance_algorithm,
                        'task_type': data.task_type
                        }
        self.to_file = 'meta.export_otu_table_by_level(otu_file)'
        self.run()
        return self.returnInfo
