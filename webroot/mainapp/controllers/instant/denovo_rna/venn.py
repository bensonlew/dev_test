# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.controllers.core.basic import Basic
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.denovo import Denovo


class Venn(Basic):
    def POST(self):
        data = web.input()
        self.data = data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        self._client = data.client
        self._mainTableId = data.express_id
        express_info = Denovo().get_main_info(data.express_id, 'sg_denovo_express')
        if express_info:
            self._taskId = express_info["task_id"]
            self._projectSn = express_info["project_sn"]
            task_info = Denovo().get_task_info(self._taskId)
            if task_info:
                self._memberId = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_id对应的表达量矩阵对应的task：{}没有member_id!".format(express_info["task_id"])}
                return json.dumps(info)
        return_info = super(Venn, self).POST()
        if return_info:
            return return_info
        self.task_name = 'denovo_rna.report.venn'
        self.to_file = ["denovo.export_express_matrix(express_file)", "denovo.export_group_table_by_detail(group_table)"]
        my_param = dict()
        my_param['express_id'] = data.express_id
        my_param['group_id'] = data.group_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param["submit_location"] = data.submit_location
        my_param["task_type"] = data.task_type
        self.params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        self.options = {
            "express_file": data.express_id,
            "group_table": data.group_id,
            "express_id": str(self._mainTableId),
            "group_detail": data.group_detail
        }
        self.run()
        return self.returnInfo

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['group_id', 'express_id', "group_detail", 'submit_location', 'task_type']
        success = []
        table_dict = json.loads(data.group_detail)
        print "收到请求, 请求的内容为："
        print data
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if not hasattr(data, "task_type"):
            success.append("缺少参数task_type")
        if data.task_type not in ["projectTask", "reportTask"]:
            success.append("参数task_type的值必须为projectTask或者是reportTask!")
        if not isinstance(table_dict, dict):
            success.append("传入的table_dict不是一个字典")
        return success
