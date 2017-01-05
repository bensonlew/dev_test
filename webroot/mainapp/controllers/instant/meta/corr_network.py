# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.corr_networkid_stat import CorrNetworkidStat as G
from mainapp.libs.param_pack import *
import re


class CorrNetwork(MetaController):
    def __int__(self):
        super(CorrNetwork, self).__int__()

    def POST(self):
        return_info = super(CorrNetwork, self).POST()
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id', 'lable', 'ratio_method', 'coefficient', 'abundance']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {'success': False, 'info': 'level{}不在规定范围内{}'.format(data.level_id)}
            return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "corr_network_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.task_name = 'meta.report.corr_network'
        self.task_type = 'workflow'
        self.main_table_name = 'Corr_Network_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        params = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'lable': float(data.lable),
            'ratio_method': data.ratio_method,
            'coefficient': float(data.coefficient),
            'abundance': int(data.abundance),
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        params = json.dumps(params, sort_keys=True, separators=(',', ':'))

        corr_network_id = G().create_corrnetwork(params=params, group_id=data.group_id, from_otu_table=data.otu_id, name=name,
                                           level_id=data.level_id)
        self.options = {
            'otutable': data.otu_id,
            'grouptable': data.group_id,
            'group_detail': data.group_detail,
            'lable': float(data.lable),
            'method': data.ratio_method,
            'level': int(data.level_id),
            'coefficient': float(data.coefficient),
            'abundance': int(data.abundance),
            'corr_network_id': str(corr_network_id)
        }
        self.to_file = ["meta.export_otu_table_by_detail(otutable)", "meta.export_group_table_by_detail(grouptable)"]
        self.run()
        return self.returnInfo
