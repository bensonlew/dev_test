# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# last modify 20170310
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from bson import ObjectId


class CorrNetwork(MetaController):
    def __init__(self):
        super(CorrNetwork, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id', 'lable', 'ratio_method', 'coefficient', 'abundance']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {'success': False, 'info': 'level{}不在规定范围内{}'.format(data.level_id)}
            return json.dumps(info)
        if int(data.level_id) in [1, 2]:
            info = {'success': False, 'info': 'OTU表中的物种数目太少，不能进行该分析，请选择Phylum以下的分类水平！'}
            return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            info = {'success': False, 'info': '传入的group_detail不是一个字典！'}
            return json.dumps(info)
        sample_num = 0
        for key in group_detail:
            sample_num += len(group_detail[key])
        if sample_num < 3:
            info = {'success': False, 'info': '样本的总个数少于3个，不能进行物种相关性网络分析！'}
            return json.dumps(info)
        otu_info = self.meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        # if otu_info:
        #     name = "corr_network_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        task_name = 'meta.report.corr_network'
        task_type = 'workflow'
        task_info = self.meta.get_task_info(otu_info['task_id'])
        level_name = ["Domain", "Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species", "OTU"]  # add by hongdongxuan 20170322
        main_table_name = 'CorrNetwork' + data.ratio_method.capitalize() + level_name[int(data.level_id) - 1] + '_' + \
                          datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        params_json = {
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
        if data.group_id == 'all':
            group__id = data.group_id
        else:
            group__id = ObjectId(data.group_id)

        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),
            ('group_id', group__id),
            ('status', 'start'),
            ('desc', 'corr_network分析'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('level_id', int(data.level_id)),
            ('params', json.dumps(params_json, sort_keys=True, separators=(',', ':'))),
            ('name', main_table_name)
        ]
        main_table_id = self.meta.insert_main_table('sg_corr_network', mongo_data)
        update_info = {str(main_table_id): 'sg_corr_network'}
        options = {
            'otutable': data.otu_id,
            'grouptable': data.group_id,
            'group_detail': data.group_detail,
            'lable': float(data.lable),
            'method': data.ratio_method,
            'level': int(data.level_id),
            'coefficient': float(data.coefficient),
            'abundance': int(data.abundance),
            'update_info': json.dumps(update_info),
            'corr_network_id': str(main_table_id)
        }
        to_file = ["meta.export_otu_table_by_detail(otutable)", "meta.export_group_table_by_detail(grouptable)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name="CorrNetwork/" + main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(CorrNetwork, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }
        }
        return json.dumps(task_info)
