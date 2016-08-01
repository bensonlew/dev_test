# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack


class OtuSubsample(MetaController):
    def POST(self):
        return_info = super(OtuSubsample, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['size', 'submit_location']
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.otu_subsample'
        self.options = {
            "input_otu_id": data.otu_id,
            "size": data.size
        }
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param["submit_location"] = data.submit_location
        my_param["size"] = data.size
        my_param["task_type"] = "reportTask"
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
