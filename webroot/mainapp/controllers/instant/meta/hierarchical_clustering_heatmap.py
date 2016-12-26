# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort


class HierarchicalClusteringHeatmap(MetaController):
    def POST(self):
        return_info = super(HierarchicalClusteringHeatmap, self).POST()
        if return_info:
            return return_info
        data = web.input()
        postArgs = ["otu_id", "level_id", "group_id", "group_detail", "species_number", "method", "task_type", "sample_method", "add_Algorithm"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        self.task_name = 'meta.report.hierarchical_clustering_heatmap'
        if data.method not in ["average", "single", "complete", ""]:
            info = {'success': False, "info": "参数method的值为{}，应该为average，single或者complete".format(data.method)}



        self.options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "level": str(data.level_id),
            "species_number": data.species_number,  # 筛选物种参数
            "method": data.method,
            "sample_method": data.sample_method,  # 样本聚类方式
            "add_Algorithm": data.add_Algorithm   # 样本求和方式
        }
        self.to_file = "meta.export_otu_table_by_level(in_otu_table)"    # 暂时不改动同样的方式导表
        my_param = dict()
        my_param['submit_location'] = data.submit_location
        my_param['otu_id'] = data.otu_id
        my_param["level_id"] = int(data.level_id)
        my_param["group_id"] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['species_number'] = data.species_number   # 筛选物种参数
        my_param["method"] = data.method
        my_param["task_type"] = data.task_type
        my_param["sample_method"] = data.sample_method
        my_param["add_Algorithm"] = data.add_Algorithm
        self.params = param_pack(my_param)
        self.run()
        return self.returnInfo
