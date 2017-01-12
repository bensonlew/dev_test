# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.controllers.core.basic import Basic
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.denovo import Denovo
from mainapp.controllers.project.denovo_controller import DenovoController


class DenovoVenn(DenovoController):
    def __init__(self):
        super(DenovoVenn, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        self.data = data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        express_info = Denovo().get_main_info(data.express_id, 'sg_denovo_express')
        if not express_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        self._taskId = express_info["task_id"]
        self._projectSn = express_info["project_sn"]
        self.task_name = 'denovo_rna.report.venn'
        my_param = dict()
        my_param['express_id'] = data.express_id
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["submit_location"] = data.submit_location
        my_param["task_type"] = data.task_type
        self.params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('status', 'start'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("level_id", int(data.level_id)),
            ("params", json.dumps(my_param, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = meta.insert_main_table('sg_species_difference_check', mongo_data)
        print main_table_id, type(main_table_id)
        update_info = {str(main_table_id): 'sg_species_difference_check'}
        self.options = {
            "express_file": data.express_id,
            "group_table": data.group_id,
            "express_id": str(self._mainTableId),
            "group_detail": data.group_detail,
            "update_info": json.dumps(update_info),
            "main_id": str(main_table_id)
        }
        to_file = ["denovo.export_express_matrix(express_file)", "denovo.export_group_table_by_detail(group_table)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name, module_type=task_type, to_file=to_file)
        task_info = super(TwoGroup, self).POST()
        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        print(self.return_msg)
        return json.dumps(task_info)

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['group_id', 'express_id', "group_detail", 'submit_location']
        # params_name = ['group_id', 'express_id', "group_detail", 'submit_location', 'task_type']
        success = []
        table_dict = json.loads(data.group_detail)
        print "收到请求, 请求的内容为："
        print data
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        # if not hasattr(data, "task_type"):
        #     success.append("缺少参数task_type")
        # if data.task_type not in ["projectTask", "reportTask"]:
        #     success.append("参数task_type的值必须为projectTask或者是reportTask!")
        if not isinstance(table_dict, dict):
            success.append("传入的table_dict不是一个字典")
        return success
