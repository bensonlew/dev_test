# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
# from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.estimator import Estimator


class EstTTest(MetaController):
    """

    """
    def __init__(self):
        super(EstTTest, self).__init__()

    def POST(self):
        return_info = super(EstTTest, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['alpha_diversity_id', 'group_detail', 'group_id', 'submit_location']  # 可以不要otu_id
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        table_dict = json.loads(data.group_detail)
        if not isinstance(table_dict, dict):
            info = {"success": False, "info": "传入的group_detail不是字典"}
            return json.dumps(info)
        if len(table_dict) < 2:
            info = {"success": False, "info": "请选择至少两组及以上的分组"}
            return json.dumps(info)
        # my_param = dict()
        # group_detail = group_detail_sort(data.group_detail)
        # my_param['group_id'] = data.group_id
        # my_param['submit_location'] = data.submit_location
        est_params = Estimator().get_est_params(data.alpha_diversity_id)
        otu_id = str(est_params[0])
        # print(my_param)
        # params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        self.task_name = 'meta.report.est_t_test'
        self.task_type = 'workflow'
        self.options = {"est_table": data.alpha_diversity_id,
                        "otu_id": otu_id,
                        "group_detail": data.group_detail,
                        "group_table": data.group_id,
                        'submit_location': data.submit_location,
                        "est_id": data.alpha_diversity_id,
                        "group_id": data.group_id,
                        "task_type": data.task_type
                        }
        self.to_file = ["estimator.export_est_table(est_table)", "meta.export_group_table_by_detail(group_table)"]
        self.run()
        return self.returnInfo
