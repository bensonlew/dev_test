# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController


class Estimators(MetaController):
    """

    """
    ESTIMATORS = ['ace', 'bergerparker', 'boneh', 'bootstrap', 'bstick', 'chao', 'coverage', 'default', 'efron',
                  'geometric', 'goodscoverage', 'heip', 'invsimpson', 'jack', 'logseries', 'npshannon', 'nseqs',
                  'qstat', 'shannon', 'shannoneven', 'shen', 'simpson', 'simpsoneven', 'smithwilson', 'sobs', 'solow']

    def __init__(self):
        super(Estimators, self).__init__()

    def POST(self):
        return_info = super(Estimators, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'index_type', 'submit_location']  # 可以不要otu_id
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        for index in data.index_type.split(','):
            if index not in self.ESTIMATORS:
                info = {"success": False, "info": "指数类型不正确{}".format(index)}
                return json.dumps(info)
        self.task_name = 'meta.report.estimators'
        self.task_type = 'workflow'
        self.options = {"otu_file": data.otu_id,
                        "otu_id": data.otu_id,
                        "indices": data.index_type,
                        "level": data.level_id,
                        "submit_location": data.submit_location
                        }
        self.to_file = 'meta.export_otu_table_by_level(otu_file)'
        self.run()
        return self.returnInfo
