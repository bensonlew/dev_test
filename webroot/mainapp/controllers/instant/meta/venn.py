# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort
from mainapp.models.mongo.public.meta.meta import Meta


class Venn(MetaController):
    def POST(self):
        return_info = super(Venn, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ['group_id', 'level_id', "group_detail", 'submit_location']
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        self.task_name = 'meta.report.venn'
        group_detal_dict = json.loads(data.group_detail)
        specimen_ids = list()
        for v in group_detal_dict.values():
            for tmp in v:
                specimen_ids.append(tmp)
        specimen_ids = ",".join(specimen_ids)
        self.options = {
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "group_table": data.group_id,
            "samples": Meta().sampleIdToName(specimen_ids),
            "level": data.level_id,
            "otu_id": str(data.otu_id)
        }
        self.to_file = ["meta.export_otu_table_by_level(in_otu_table)", "meta.export_group_table_by_detail(group_table)"]
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = int(data.level_id)
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["submit_location"] = data.submit_location
        my_param["task_type"] = data.task_type
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
