# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.controllers.project.meta_controller import MetaController


class DistanceCalc(MetaController):
    def __init__(self):
        super(DistanceCalc, self).__init__()

    def POST(self):  # 建议不要在controller中写mongo的主表初始化表，统一在api.database中写入
        return_info = super(DistanceCalc, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        # data = self.data  # for test
        default_argu = ['otu_id', 'level_id', 'distance_algorithm', 'submit_location']  # 可以不要otu_id
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        self.task_name = 'meta.report.distance_calc'
        self.task_type = 'workflow'  # 可以不配置
        self.options = {'otu_file': data.otu_id,
                        'otu_id': data.otu_id,
                        'level': int(data.level_id),
                        'method': data.distance_algorithm
                        }
        self.to_file = 'meta.export_otu_table_by_level(otu_file)'
        self.run()
        return self.returnInfo

# if __name__ == '__main__':
#     a = DistanceCalc()
#
#
#     class data_class_test(object):
#         def __init__(self):
#             self.otu_id = '56ce51860e6da9cf6bd716f3'
#             self.level_id = 8
#             self.distance_algorithm = 'unifrac'
#             self.submit_location = 'submit_location'
#             self.client = 'client01'
#
#     data = data_class_test()
#     a.data = data
#     a.POST()
