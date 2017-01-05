# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
import datetime
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
        default_argu = ['otu_id', 'level_id', 'index_type', 'submit_location', "group_id"]  # 可以不要otu_id
        index_types = []
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        for index in data.index_type.split(','):
            index_types.append(index)
            if index not in self.ESTIMATORS:
                info = {"success": False, "info": "指数类型不正确{}".format(index)}
                return json.dumps(info)
        self.task_name = 'meta.report.estimators'
        self.task_type = 'workflow'
        self.main_table_name = 'Estimators_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.options = {"otu_file": data.otu_id,
                        "otu_id": data.otu_id,
                        "indices": data.index_type,
                        "level": data.level_id,
                        "submit_location": data.submit_location,
                        "task_type": data.task_type,
                        "group_detail": data.group_detail,
                        "group_id": data.group_id
                        }
        # self.to_file = 'meta.export_otu_table_by_level(otu_file)'
        self.to_file = 'meta.export_otu_table_by_detail(otu_file)'
        self.run()
        # print self.returnInfo
        return_info = json.loads(self.returnInfo)
        return_info['content']["ids"]["index_types"] = index_types
        # print(return_info['content']["ids"]["index_types"])
        # print(return_info)
        # return json.dumps(return_info)
