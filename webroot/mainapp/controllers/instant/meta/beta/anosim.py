# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import web
import json
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController
import datetime


class Anosim(MetaController):
    def __init__(self):
        super(Anosim, self).__init__()

    def POST(self):  # 建议不要在controller中写mongo的主表初始化表，统一在api.database中写入
        return_info = super(Anosim, self).POST()  # 初始化出错才会返回
        if return_info:
            return return_info
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'distance_algorithm',
                        'permutations', 'group_id', 'group_detail',
                        'submit_location']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        try:
            group = json.loads(data.group_detail)
        except ValueError:
            info = {'success': False, 'info': 'group_detail格式不正确!:%s' % data.group_detail}
            return json.dumps(info)
        try:
            int(data.permutations)
        except ValueError:
            info = {'success': False, 'info': 'permutations格式应该为数字!:%s' % data.permutations}
            return json.dumps(info)
        if not (9 < int(data.permutations) < 10000):
            info = {'success': False, 'info': '置换次数应该在[10-10000]之间:%s' % data.permutations}
            return json.dumps(info)
        if len(group) < 2:
            info = {'success': False, 'info': '不可只选择一个分组'}
            return json.dumps(info)
        samples = reduce(lambda x, y: x + y, group.values())
        if len(samples) == len(set(samples)):
            pass
        else:
            info = {'success': False, 'info': '不同分组存在相同的样本id'}
            return json.dumps(info)
        if len(samples) <= len(group):
            info = {'success': False, 'info': '不可每个组都只含有一个样本'}
            return json.dumps(info)

        self.task_name = 'meta.report.anosim'
        self.task_type = 'workflow'  # 可以不配置
        self.main_table_name = 'Anosim&anosim_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'distance_algorithm': data.distance_algorithm,
            'permutations': data.permutations,
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': data.task_type
            }
        self.options = {
            'otu_file': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'method': data.distance_algorithm,
            'group_file': data.group_id,
            'group_id': data.group_id,
            'group_detail': data.group_detail,
            'samples': ','.join(samples),
            'permutations': int(data.permutations),
            'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            }
        self.to_file = ['meta.export_otu_table_by_level(otu_file)',
                        'meta.export_group_table_by_detail(group_file)']
        self.run()
        return self.returnInfo
