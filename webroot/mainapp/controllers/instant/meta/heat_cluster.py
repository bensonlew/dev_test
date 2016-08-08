# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import re
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack
from mainapp.models.mongo.public.meta.meta import Meta


class HeatCluster(MetaController):
    def POST(self):
        return_info = super(HeatCluster, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['group_id', 'level_id', 'submit_location', 'group_detail', 'linkage']
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.heat_cluster'
        tmp = json.loads(data.group_detail).values()
        sampleIds = list()
        for sp in tmp:
            sampleIds.extend(sp)
        sampleIds = map(str, sampleIds)
        sampleIds = ",".join(sampleIds)
        self.options = {
            "in_otu_table": data.otu_id,
            "samples": Meta().sampleIdToName(sampleIds),
            "linkage": data.linkage,
            "level": int(data.level_id)
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        c_name = re.split(',', sampleIds)
        c_name.sort()
        new_cname = ','.join(c_name)
        my_param['sampleIds'] = new_cname
        my_param["submit_location"] = data.submit_location
        my_param["linkage"] = data.linkage
        my_param["taskType"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
