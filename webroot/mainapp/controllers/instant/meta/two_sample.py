# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.group_stat import GroupStat as G
from mainapp.libs.param_pack import group_detail_sort


class TwoSample(MetaController):
    def __init__(self):
        super(TwoSample, self).__init__()

    def POST(self):
        return_info = super(TwoSample, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            print json.dumps(info)
            return json.dumps(info)
        self.task_name = 'meta.report.two_sample'
        self.task_type = 'workflow'  # 可以不配置
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = int(data.level_id)
        my_param['sample1'] = data.sample1
        my_param['sample2'] = data.sample2
        my_param['ci'] = float(data.ci)
        my_param['correction'] = data.correction
        my_param['type'] = data.type
        my_param['test'] = data.test
        my_param['methor'] = data.methor
        my_param['coverage'] = float(data.coverage)
        my_param['submit_location'] = data.submit_location
        my_param['task_type'] = 'reportTask'
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        self.options = {"otu_file": data.otu_id,
                        "level": data.level_id,
                        "test": data.test,
                        "correction": data.correction,
                        "ci": data.ci,
                        "type": data.type,
                        "sample1": data.sample1,
                        "sample2": data.sample2,
                        "methor": data.methor,
                        "coverage": float(data.coverage),
                        "params": params
                        }
        self.to_file = "meta.export_otu_table_by_level(otu_file)"
        self.run()
        return self.returnInfo

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'level_id', 'sample1', 'sample2', 'ci', 'correction', 'type', 'test', 'methor', 'coverage', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if int(data.level_id) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append("level_id不在范围内")
        if data.correction not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr", "none"]:
            success.append("多重检验方法不在范围内")
        if data.type not in ["two.side", "greater", "less"]:
            success.append("检验类型不在范围内")
        if float(data.ci) > 1 or float(data.ci) < 0:
            success.append("显著性水平不在范围内")
        if data.test not in ["chi", "fisher"]:
            success.append("所选的分析检验方法不在范围内")
        if float(data.coverage) not in [0.90, 0.95, 0.98, 0.99, 0.999]:
            success.append('置信区间的置信度不在范围值内')
        if data.methor not in ["DiffBetweenPropAsymptoticCC", "DiffBetweenPropAsymptotic", "NewcombeWilson"]:
            success.append('计算置信区间的方法不在范围值内')
        sample_name = G().get_otu_sample_name(data.otu_id)
        if data.sample1 not in sample_name or data.sample2 not in sample_name:
            success.append('所输入的样本名不在otu表里，请检查样本名')
        return success
