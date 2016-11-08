# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
# from mainapp.libs.param_pack import group_detail_sort


class MantelTest(MetaController):
    """
    mantel_test检验接口
    """
    MATRIX = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord', 'euclidean',
              'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn', 'pearson', 'soergel', 'spearman_approx',
              'specprof', 'unifrac', 'unweighted_unifrac', 'weighted_normalized_unifrac', 'weighted_unifrac']

    MATRIXFACTOR = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming',
                    'binary_jaccard', 'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson',
                    'binary_sorensen_dice', 'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra',
                    'chisq', 'chord', 'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
                    'pearson', 'soergel', 'spearman_approx', 'specprof']

    def __init__(self):
        super(MantelTest, self).__init__()

    def POST(self):
        return_info = super(MantelTest, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', "group_id", "units", "env_id", "otu_method", "env_method", "env_labs"]

        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)

        if data.otu_method not in self.MATRIX:
            info = {'success': False, 'info': '选择计算OTU表矩阵的方法不正确%s!' % data.otu_method}
            return json.dumps(info)
        if data.env_method not in self.MATRIXFACTOR:
            info = {'success': False, 'info': '选择计算环境因子表样本距离矩阵s的方法不正确%s!' % data.otu_method}
            return json.dumps(info)

        # group_detail = group_detail_sort(data.group_detail)
        print(data.group_detail)

        self.task_name = 'meta.report.mantel_test'
        self.task_type = 'workflow'
        self.options = {"otu_file": data.otu_id,
                        "otu_id": data.otu_id,
                        "level": data.level_id,
                        "submit_location": data.submit_location,
                        "task_type": data.task_type,
                        "group_detail": data.group_detail,
                        "group_id": data.group_id,
                        "env_id": data.env_id,
                        "env_file": data.env_id,
                        "otu_method": data.otu_method,
                        "env_method": data.env_method,
                        "units": data.units,
                        "env_labs": data.env_labs
                        }
        self.options["params"] = str(self.options)
        self.to_file = ['meta.export_otu_table_by_detail(otu_file)', "env.export_env_table(env_file)"]
        self.run()
        # print self.returnInfo
        return_info = json.loads(self.returnInfo)
        return json.dumps(return_info)