# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

import web
import json
import datetime
from bson import ObjectId
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController


class FunctionPredict(MetaController):
    def __init__(self):
        super(FunctionPredict, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        print data
        default_argu = ['otu_id', 'submit_location', "group_id", "group_detail", "group_method", "task_type"]
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {"success": False, "info": "缺少参数%s!" % argu}
                return json.dumps(info)
        if data.group_method not in ["", "sum", "average", "middle"]:
            info = {"success": False, "info": "对分组样本计算方式:%s错误!" % data.group_method}
            return json.dumps(info)
        task_name = 'meta.report.function_predict'
        task_type = 'workflow'
        otu_info = self.meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = self.meta.get_task_info(otu_info["task_id"])
        params_json = {
            'otu_id': data.otu_id,
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'group_method': data.group_method,
            'task_type': data.task_type,
            'submit_location': data.submit_location
        }
        main_table_name = "16sFunctionPrediction_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('status', 'start'),
            ('desc', '16s功能预测分析'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('params', json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = self.meta.insert_main_table('sg_16s', mongo_data)
        update_info = {str(main_table_id): 'sg_16s'}
        options = {
            "update_info": json.dumps(update_info),
            "otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "group_method": data.group_method,
            "predict_id": str(main_table_id)
        }
        to_file = ["function_predict.export_otu_table_by_detail(otu_table)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name="16sFunctionPrediction/" + main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(FunctionPredict, self).POST()
        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        print task_info
        return json.dumps(task_info)
