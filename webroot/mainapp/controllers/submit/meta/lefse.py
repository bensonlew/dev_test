# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.group_stat import GroupStat as G
from mainapp.libs.param_pack import *
import re


class Lefse(MetaController):
    def __init__(self):
        super(Lefse, self).__init__()

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        if data.second_group_detail:
            my_param['second_group_detail'] = group_detail_sort(data.second_group_detail)
        else:
            my_param['second_group_detail'] = data.second_group_detail
        my_param['group_id'] = data.group_id
        my_param['second_group_id'] = data.second_group_id
        if re.search(r'\.0$', data.lda_filter):
            my_param['lda_filter'] = int(float(data.lda_filter))
        elif re.search(r'\..*$', data.lda_filter):
            my_param['lda_filter'] = float(data.lda_filter)
        else:
            my_param['lda_filter'] = int(data.lda_filter)
        my_param['strict'] = int(data.strict)
        my_param['submit_location'] = data.submit_location
        my_param['task_type'] = data.task_type
        my_param['start_level'] = int(data.start_level)
        my_param['end_level'] = int(data.end_level)
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "lefse_lda_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            lefse_id = G().create_species_difference_lefse(params, data.group_id, data.otu_id, name)
            update_info = {str(lefse_id): "sg_species_difference_lefse"}
            update_info = json.dumps(update_info)
            options = {
                "otu_file": data.otu_id,
                "update_info": update_info,
                "group_file": data.group_id,
                "group_detail": data.group_detail,
                "second_group_detail": data.second_group_detail,
                "group_name": G().get_group_name(data.group_id, lefse=True, second_group=data.second_group_detail),
                "strict": data.strict,
                "lda_filter": data.lda_filter,
                "lefse_id": str(lefse_id),
                "start_level": int(data.start_level),
                "end_level": int(data.end_level),
            }
            to_file = ["meta.export_otu_table(otu_file)", "meta.export_cascading_table_by_detail(group_file)"]
            self.set_sheet_data(name='meta.report.lefse', options=options, main_table_name=name, module_type='workflow', to_file=to_file)
            task_info = super(Lefse, self).POST()
            task_info['content'] = {'ids': {'id': str(lefse_id), 'name': name}}
            print task_info
            return json.dumps(task_info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'submit_location', 'group_detail', 'group_id', 'lda_filter', 'strict', 'second_group_detail', 'task_type', 'second_group_id', 'start_level', 'end_level']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if int(data.strict) not in [1, 0]:
            success.append("严格性比较策略不在范围内")
            return json.dumps(info)
        if float(data.lda_filter) > 4.0 or float(data.lda_filter) < -4.0:
            success.append("LDA阈值不在范围内")
        if int(data.start_level) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append('起始分类水平不在范围内')
        if int(data.end_level) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append('结束分类水平不在范围内')
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        if data.second_group_detail != '':
            second_group_detail = json.loads(data.second_group_detail)
            first = 0
            second = 0
            for i in group_detail.values():
                first += len(i)
            for n in second_group_detail.values():
                second += len(n)
            if not isinstance(second_group_detail, dict):
                success.append("传入的second_group_detail不是一个字典")
            if first != second:
                success.append("二级分组与一级分组的样本数不相同，请检查！")
        return success
