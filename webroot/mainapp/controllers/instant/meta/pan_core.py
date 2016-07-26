# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort
from mainapp.models.mongo.public.meta.meta import Meta


class PanCore(MetaController):
    def POST(self):
        return_info = super(PanCore, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['group_id', 'level_id', 'submit_location', "group_detail"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.pan_core'
        # data.group_detail是一个字符串，示例如下
        # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
        group_detal_dict = json.loads(data.group_detail)
        specimen_ids = list()
        for v in group_detal_dict.values():
            for tmp in v:
                specimen_ids.append(tmp)
        specimen_ids = ",".join(specimen_ids)
        self.options = {
            "in_otu_table": data.otu_id,
            "group_table": data.group_id,
            "group_detail": data.group_detail,
            "samples": Meta().sampleIdToName(specimen_ids),
            "level": int(data.level_id)
        }
        self.to_file = ["meta.export_otu_table_by_level(in_otu_table)", "meta.export_group_table_by_detail(group_table)"]
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["submit_location"] = data.submit_location
        my_param["taskType"] = "reportTask"
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
