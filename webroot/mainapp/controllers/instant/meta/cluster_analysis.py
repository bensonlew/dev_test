# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class ClusterAnalysis(MetaController):
    def POST(self):
        return_info = super(ClusterAnalysis, self).POST()
        if return_info:
            return return_info
        data = web.input()
        # postArgs = ["group_detail", "method", "otu_id", "level_id", "task_type", "group_id"]
        postArgs = ["group_detail", "otu_id", "level_id", "task_type", "group_id"]  # modify by zhouxuan 2016.11.22
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        self.task_name = 'meta.report.cluster_analysis'
        # if data.method not in ["average", "single", "complete", ""]:
        #     info = {'success': False, "info": "参数method的值为{}，应该为average，single, complete或者是''".format(data.method)}
        # modify by zhouxuan 2016.11.22

        self.options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "level": str(data.level_id),
            # "method": data.method # modify by zhouxuan 2016.11.22
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"
        my_param = dict()
        my_param['submit_location'] = data.submit_location
        my_param['otu_id'] = data.otu_id
        my_param["level_id"] = int(data.level_id)
        my_param["group_id"] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        # my_param["method"] = data.method # modify by zhouxuan 2016.11.22
        my_param["task_type"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
