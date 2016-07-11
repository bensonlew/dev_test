# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import re
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack


class Venn(MetaController):
    def POST(self):
        return_info = super(Venn, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['group_id', 'level_id', "category_name", 'submit_location']
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.venn'
        self.options = {
            "in_otu_table": data.otu_id,
            "category_name": data.category_name,
            "group_table": data.group_id,
            "level": data.level_id,
            "otu_id": str(data.otu_id)
        }
        self.to_file = ["meta.export_otu_table_by_level(in_otu_table)", "meta.export_group_table(group_table)"]
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        c_name = re.split(',', data.category_name)
        c_name.sort()
        new_cname = ','.join(c_name)
        my_param['category_name'] = new_cname
        my_param["submit_location"] = data.submit_location
        my_param["taskType"] = "reportTask"
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
