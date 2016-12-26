# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class OtuSubsample(MetaController):
    def POST(self):
        return_info = super(OtuSubsample, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['size', 'submit_location', "otu_id", "task_type", "group_detail", "group_id", "filter_json"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.otu_subsample'
        self.main_table_name = 'OTU_Subsample_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.options = {
            "in_otu_table": data.otu_id,
            "input_otu_id": data.otu_id,
            "group_detail": data.group_detail,
            "filter_json": data.filter_json,
            "level": "9",
            "size": data.size
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"
        my_param = dict()
        my_param["group_id"] = data.group_id
        my_param['otu_id'] = data.otu_id
        my_param["submit_location"] = data.submit_location
        my_param["size"] = data.size
        my_param["filter_json"] = json.loads(data.filter_json)
        my_param["group_detail"] = group_detail_sort(data.group_detail)
        my_param["task_type"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
