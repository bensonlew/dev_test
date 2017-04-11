# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class OtuStatic(MetaController):
    def POST(self):
        return_info = super(OtuStatic, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['group_id', 'submit_location', "group_detail", "size", "method", "otu_id"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.otu_static'
        self.main_table_name = 'OTU_Static_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        specimen_ids = list()
        group_detal_dict = json.loads(data.group_detail)
        for v in group_detal_dict.values():
            for tmp in v:
                specimen_ids.append(tmp)
        specimen_ids = ",".join(specimen_ids)
        self.options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "size": str(data.size),
            "level": "9",
            "method": data.method
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["size"] = data.size
        my_param["method"] = data.method
        my_param["submit_location"] = data.submit_location
        my_param["task_type"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
