# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.estimator import Estimator
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.meta import Meta
from bson import ObjectId
import datetime


class EstTTest(MetaController):
    """

    """
    def __init__(self):
        super(EstTTest, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['alpha_diversity_id', 'group_detail', 'group_id', 'submit_location', 'test_method']  # 可以不要otu_id
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        table_dict = json.loads(data.group_detail)
        if not isinstance(table_dict, dict):
            info = {"success": False, "info": "传入的group_detail不是字典"}
            return json.dumps(info)
        if len(table_dict) < 2:
            info = {"success": False, "info": "请选择至少两组以上的分组"}
            return json.dumps(info)
        for key in table_dict:
            if len(table_dict[key]) < 2:
                info = {"success": False, "info": "每组至少有两个样本"}
                return json.dumps(info)

        task_name = 'meta.report.est_t_test'
        task_type = 'workflow'
        meta = Meta()

        params_json = {
            'alpha_diversity_id': data.alpha_diversity_id,
            "otu_id": data.otu_id,
            "submit_location": data.submit_location,
            "task_type": data.task_type,
            "group_id": data.group_id,
            "group_detail": group_detail_sort(data.group_detail)
            }
        if hasattr(data, "test_method"):
            params_json["test_method"] = data.test_method

        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        main_table_name = 'EstimatorStat_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('alpha_diversity_id', ObjectId(data.alpha_diversity_id)),
            ('status', 'start'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = meta.insert_main_table('sg_alpha_ttest', mongo_data)
        update_info = {str(main_table_id): 'sg_alpha_ttest'}
        options = {
            "est_table": data.alpha_diversity_id,
            "group_table": data.group_id,
            'update_info': json.dumps(update_info),
            "est_id": data.alpha_diversity_id,
            "group_detail": data.group_detail,
            "est_t_test_id": str(main_table_id),
            # "est_test_method": data.test_method
                    }
        if hasattr(data, "test_method"):
            params_json["est_test_method"] = data.test_method
        del params_json["group_detail"]
        if hasattr(data, "test_method"):
            del params_json["test_method"]
        del params_json["alpha_diversity_id"]
        options.update(params_json)
        to_file = ["estimator.export_est_table(est_table)", "meta.export_group_table_by_detail(group_table)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name="Estimators/" + main_table_name,
                            module_type=task_type, to_file=to_file)  # modified by hongdongxuan 20170322 在main_table_name前面加上文件输出的文件夹名
        task_info = super(EstTTest, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
                }}
        print(task_info)
        if not task_info["success"]:
            task_info["info"] = "程序运行出错，请检查输入的多样性指数表是否存在异常（样本值完全相同或是存在NA值等情况）"
        return json.dumps(task_info)
