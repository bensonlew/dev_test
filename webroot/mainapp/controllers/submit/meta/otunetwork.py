# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# last modify 20170310
import web
import json
import datetime
from mainapp.models.mongo.meta import Meta
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import *
from bson import ObjectId


class Otunetwork(MetaController):
    def __init__(self):
        super(Otunetwork, self).__init__(instant=False)

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
        if sample_num < 2:
            info = {'success': False, 'info': '样本的总个数少于2个，不能进行共现性网络分析！'}
            return json.dumps(info)
        otu_info = self.meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_name = 'meta.report.otunetwork'
        task_type = 'workflow'
        task_info = self.meta.get_task_info(otu_info['task_id'])
        main_table_name = 'OTUNetwork_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': data.task_type
        }
        params = json.dumps(params_json, sort_keys=True, separators=(',', ':'))
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
            ('desc', 'otu_network分析'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('level_id', int(data.level_id)),
            ('params', params),
            ('name', main_table_name)
        ]
        main_table_id = Meta().insert_main_table('sg_network', mongo_data)
        update_info = {str(main_table_id): 'sg_network'}
        options = {
            "otutable": data.otu_id,
            "grouptable": data.group_id,
            "group_detail": data.group_detail,
            "update_info": json.dumps(update_info),
            "group_id": data.group_id,
            "level": int(data.level_id),
            "network_id": str(main_table_id)
        }
        to_file = ["meta.export_otu_table_by_detail(otutable)", "meta.export_group_table_by_detail(grouptable)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name="OTUnetwork/" + main_table_name,
                            module_type=task_type, to_file=to_file) # modified by hongdongxuan 20170322 在main_table_name前面加上文件输出的文件夹名
        task_info = super(Otunetwork, self).POST()
        # print "+++++..."
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }
        }
        # print task_info
        return json.dumps(task_info)
