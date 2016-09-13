# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.group_stat import GroupStat as G
from mainapp.libs.param_pack import group_detail_sort


class Multiple(MetaController):
    def __init__(self):
        super(Multiple, self).__init__()

    def POST(self):
        return_info = super(Multiple, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        self.task_name = 'meta.report.multiple'
        self.task_type = 'workflow'  # 可以不配置
        groupname = json.loads(data.group_detail).keys()
        groupname.sort()
        category_name = ','.join(groupname)
        print category_name
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = int(data.level_id)
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['group_id'] = data.group_id
        my_param['correction'] = data.correction
        my_param['test'] = data.test
        my_param['methor'] = data.methor
        my_param['coverage'] = float(data.coverage)
        my_param['submit_location'] = data.submit_location
        my_param['task_type'] = 'reportTask'
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        self.options = {"otu_file": data.otu_id,
                        "params": params,
                        "level": data.level_id,
                        "group_file": data.group_id,
                        "correction": data.correction,
                        "test": data.test,
                        "group_name": G().get_group_name(data.group_id),
                        "methor": data.methor,
                        "coverage": data.coverage,
                        "group_detail": data.group_detail,
                        "category_name": category_name
                        }
        self.to_file = ["meta.export_otu_table_by_level(otu_file)", "meta.export_group_table_by_detail(group_file)"]
        self.run()
        return self.returnInfo

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'level_id', 'group_detail', 'group_id', 'correction', 'test',
                       'methor', 'coverage', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if int(data.level_id) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append("level_id不在范围内")
        if data.correction not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr", "none"]:
            success.append("多重检验方法不在范围内")
        if data.test not in ["kru_H", "anova"]:
            success.append("所选的分析检验方法不在范围内")
        if float(data.coverage) not in [0.90, 0.95, 0.98, 0.99, 0.999]:
            success.append('置信区间的置信度coverage不在范围值内')
        if data.methor not in ["scheffe", "welchuncorrected", "tukeykramer", "gameshowell"]:
            success.append('置信区间的方法methor不在范围值内')
        table_dict = json.loads(str(data.group_detail))
        if isinstance(table_dict, str):
            table_dict = eval(table_dict)
        if not isinstance(table_dict, dict):
            success.append("传入的table_dict不是一个字典")
        return success
