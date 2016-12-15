# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class Enterotyping(MetaController):
    def POST(self):
        return_info = super(Enterotyping, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ["otu_id", "level_id", "group_id", "group_detail", "task_type", "submit_location"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        self.task_name = 'meta.report.enterotyping'

        self.options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "level": str(data.level_id),
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"    # 暂时不改动同样的方式导表
        my_param = dict()
        my_param['submit_location'] = data.submit_location
        my_param['otu_id'] = data.otu_id
        my_param["level_id"] = int(data.level_id)
        my_param["group_id"] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["task_type"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
