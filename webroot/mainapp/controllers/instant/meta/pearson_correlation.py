# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort


class PearsonCorrelation(MetaController):
    """
    pearson 相关系数分析接口
    """

    def __init__(self):
        super(PearsonCorrelation, self).__init__()

    def POST(self):
        return_info = super(PearsonCorrelation, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', "group_id", "env_id", "env_labs"]

        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)

        # group_detail = group_detail_sort(data.group_detail)
        self.task_name = 'meta.report.pearson_correlation'
        self.task_type = 'workflow'
        self.options = {"otu_file": data.otu_id,
                        "otu_id": data.otu_id,
                        "level": int(data.level_id),
                        "submit_location": data.submit_location,
                        "task_type": data.task_type,
                        "group_detail": data.group_detail,
                        "group_id": data.group_id,
                        "env_id": data.env_id,
                        "env_file": data.env_id,
                        "env_labs": data.env_labs
                        # "method": "pearsonr"
                        }
        if hasattr(data, "method"):
            print(data.method)
            self.options["method"] = data.method
        self.options["params"] = str(self.options)
        self.to_file = ['meta.export_otu_table_by_detail(otu_file)', "env.export_float_env(env_file)"]
        self.run()
        # print self.returnInfo
        return_info = json.loads(self.returnInfo)
        return json.dumps(return_info)
