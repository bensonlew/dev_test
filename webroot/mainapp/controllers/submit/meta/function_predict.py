# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

import web
import json
import datetime
from bson import ObjectId
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.meta import Meta


class FunctionPredict(MetaController):
    def __init__(self):
        super(FunctionPredict, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'submit_location', "group_id", "group_detail", "task_type"]
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {"success": False, "info": "缺少参数%s!" % argu}
        task_name = 'meta.report.function_predict'
        task_type = 'workflow'
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = Meta().get_task_info(otu_info["task_id"])
        params_json = {
            'otu_id': data.otu_id,
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'task_type': data.task_type,
            'submit_location': data.submit_location
        }
        main_table_name = "16s_Function_Prediction_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
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
        main_table_id = Meta().insert_main_table('sg_16s', mongo_data)
        update_info = {str(main_table_id): 'sg_16s'}
        options = {
            "update_info": json.dumps(update_info),
            "otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "predict_id": str(main_table_id)
        }
        to_file = ["function_predict.export_otu_table_by_detail(otu_table)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(FunctionPredict, self).POST()
        return json.dumps(task_info)