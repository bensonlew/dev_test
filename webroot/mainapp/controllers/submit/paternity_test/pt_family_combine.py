# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import web
import json
import datetime
from mainapp.models.mongo.submit.paternity_test_mongo import PaternityTest as PT
from mainapp.controllers.project.pt_controller import PtController
from mainapp.libs.param_pack import *


class PtFamilyCombine(PtController):
    def __init__(self):
        super(PtFamilyCombine, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        task_name = 'paternity_test.report.pt_family_combine'
        task_type = 'workflow'
        params_json = {
            'dad_group': data.dad_group,
            'mom_id': data.mom_id,
            'preg_id': data.preg_id,
            'member_id': data.member_id,
            'dedup_all': data.dedup_all,
            'err_min': data.err_min,
        }
        if hasattr(data, 'new_mom_id'):
            params_json['new_mom_id'] = data.new_mom_id
            params_json['new_dad_id'] = data.new_dad_id
            params_json['new_preg_id'] = data.new_preg_id
        if hasattr(data, 'dad_id'):
            params_json['dad_id'] = data.dad_id
        if hasattr(data, 'dedup_start'):
            params_json['dedup_start'] = data.dedup_start
            params_json['dedup_end'] = data.dedup_end
        params = json.dumps(params_json, sort_keys=True, separators=(',', ':'))
        if hasattr(data, 'dad_id'):
            task_id = data.dad_group + '_' + data.dad_id + '_' + data.mom_id + '_' + data.preg_id + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        else:
            task_id = data.dad_group + '__' + data.mom_id + '_' + data.preg_id + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        mongo_data = [
            ('params', params),
            ('name', task_id),
            ('desc', '亲子鉴定家系自由组合'),
            ('member_id', data.member_id),
            ('status', 'start'),
            ('time', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        main_table_id = PT().insert_main_table('sg_pt_family_combine', mongo_data)
        update_info = {str(main_table_id): 'sg_pt_family_combine'}
        options = {
            'dad_group': data.dad_group,
            'mom_id': data.mom_id,
            'preg_id': data.preg_id,
            'err_min': data.err_min,
            'dedup_all': data.dedup_all,
            "update_info": json.dumps(update_info),
        }
        if hasattr(data, 'dad_id'):
            options['dad_id'] = data.dad_id
        if hasattr(data, 'new_mom_id'):
            options['new_mom_id'] = data.new_mom_id
            options['new_dad_id'] = data.new_dad_id
            options['new_preg_id'] = data.new_preg_id
        if hasattr(data, 'dedup_start'):
            options['dedup_start'] = data.dedup_start
            options['dedup_end'] = data.dedup_end
        sheet_data = self.set_sheet_data_(name=task_name, options=options, module_type=task_type, params=params)
        print "*********"
        print sheet_data
        task_info = super(PtFamilyCombine, self).POST()
        return json.dumps(task_info)

    def check_options(self, data):
        """
        检查网页端传入的参数
        :param data:网页端传入的数据(是否全库查重的参数必须传递)
        :return: 检查结果
        """
        params_name = ['mom_id', 'dad_group', 'preg_id',  'err_min', 'member_id', 'dedup_all']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数{}".format(names))
        WQ_number = ['new_mom_id', 'new_dad_id', 'new_preg_id']
        num = 0
        for i in WQ_number:
            if not(hasattr(data, i)):
                num += 1
        if num == 3:
            pass
        elif num == 0:
            if not hasattr(data, 'dad_id'):
                success.append("替换编号时必须要有父本id")
        else:
            success.append("新编号参数不全")
        if data.dedup_all == 'false':
            if hasattr(data, 'dedup_start'):
                if not hasattr(data, 'dedup_end'):
                    success.append("查重参数不全".format(names))
        return success



