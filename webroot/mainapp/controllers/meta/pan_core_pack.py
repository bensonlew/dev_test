# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import random
import datetime
import re
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.pan_core import PanCore as P
from mainapp.libs.param_pack import param_pack, GetUploadInfo


class PanCore(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        param_list = ["group_id", "category_name", "otu_id", "level_id", "submit_location"]
        for my_p in param_list:
            if not hasattr(data, my_p):
                info = {"success": False, "info": "缺少参数{}!".format(my_p)}
                return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        c_name = re.split(',', data.category_name)
        c_name.sort()
        new_cname = ','.join(c_name)
        my_param['category_name'] = new_cname
        my_param["submit_location"] = data.submit_location
        params = param_pack(my_param)
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
