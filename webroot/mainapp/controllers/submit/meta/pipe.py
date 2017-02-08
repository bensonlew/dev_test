# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
import web
import json
import datetime
from mainapp.models.mongo.meta import Meta
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import *
from bson import ObjectId


class Pipe(MetaController):
    def __init__(self):
        super(Pipe, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        params_name = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id']
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!!" % param}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {"success": False, "info": "level{}不在规定范围内{}".format(data.level_id)}
            return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_name = 'meta.report.meta_pipeline'
        task_type = 'workflow'
        task_info = Meta().get_task_info(otu_info['task_id'])
        main_table_name = 'pipeline_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        params = json.dumps(params_json, sort_keys=True, separators=(',', ':'))
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('batch_id', main_table_name),
            ('status', 'start'),
            ('desc', 'pipe_analysis.....'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('params', params)
        ]
        main_table_id = Meta().insert_main_table('sg_pipe_batch', mongo_data)
        update_info = {str(main_table_id): 'sg_pipe_batch'}
        options = {
            "otu_id": data.otu_id,
            "group_detail": data.group_detail,
            "update_info": json.dumps(update_info),
            "group_id": data.group_id,
            "level": int(data.level_id),
            "pipe_id": str(main_table_id),
            "submit_location": "otunetwork_analysis",
            "task_type": "reportTask"
        }
        # to_file = ["meta.export_otu_table_by_detail(otutable)", "meta.export_group_table_by_detail(grouptable)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type)
        task_info = super(Pipe, self).POST()
        # print "+++++..."
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }
        }
        return json.dumps(task_info)
