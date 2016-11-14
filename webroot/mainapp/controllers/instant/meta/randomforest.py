# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class Randomforest(MetaController):
    def __init__(self):
        super(Randomforest,self).__init__()
        
    def POST(self):
        return_info = super(Randomforest, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ["task_type", "level_id", "otu_id", "group_id", "group_detail", "submit_location", "ntree", "top_number"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        self.task_name = 'meta.report.randomforest'
        self.task_type = 'workflow'


        params = {
            'otu_id': data.otu_id,
            'group': data.group,
            'group_detail': data.group_detail,
            'ntree_id': data.ntree_id,
            'level_id': data.level_id,
            'top_number_id':data.top_number_id,
            'submit_location':submit_location,
            'task_type':'reportTask'
        }
        params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        self.options = {'otutable': data.otu_id,
                        'level': data.level_id,
                        'ntree': int(data.ntree_id),
                        'top_number': int(data.top_number_id),
                        'grouptable': data.group,
                        'group_detail': data.group_detail,
                        'params': params
                        }
        
        self.to_file = ['meta.export_otu_table_by_detail(otutable)', 'meta.export_group_table_by_detail(grouptable)']

        self.run()
        return self.returnInfo
