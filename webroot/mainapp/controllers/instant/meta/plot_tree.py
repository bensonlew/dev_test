# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort


class PlotTree(MetaController):
    def __init__(self):
        super(PlotTree, self).__init__()

    def POST(self):
        return_info = super(PlotTree, self).POST()
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'color_level_id','group_id' ,'group_detail', 'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        if int(data.level_id) < int(data.color_level_id):
            info = {'success': False, 'info': '颜色设置水平必须高于进化树绘制水平!'}
            return json.dumps(info)
        self.task_name = 'meta.report.plot_tree'
        self.task_type = 'workflow'  # 可以不配置
        params = {
            'otu_id': data.otu_id,
            'level_id': data.level_id,
            'color_level_id': data.color_level_id,
            'group_id': data.group_id,
            'group_detail':group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        self.options = {'otu_table': data.otu_id,
                        'otu_id': data.otu_id,
                        'level': int(data.level_id),
                        'color_level_id': int(data.color_level_id),
                        'sample_group': data.group_id,
                        'group_detail': data.group_detail,
                        'params': params
                        }
        self.to_file = ['meta.export_otu_table_by_detail(otu_table)', 'meta.export_group_table_by_detail(sample_group)']
        self.run()
        return self.returnInfo
