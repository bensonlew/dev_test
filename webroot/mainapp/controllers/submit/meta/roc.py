# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
import web
import json
from mainapp.libs.param_pack import group_detail_sort
from mainapp.controllers.project.meta_controller import MetaController
from bson import ObjectId
import datetime



class Roc(MetaController):
    def __init__(self):
        super(Roc, self).__init__(instant=False)
        # super(Roc, self).__init__()

    def POST(self):
        data = web.input()
        default_argu = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id', 'method_type', 'top_n_id']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)
        table_dict = json.loads(data.group_detail)
        if len(table_dict) != 2:
            info = {"success": False, "info": "分析只适用于分组方案的分组类别数量为2的情况！"}
            return json.dumps(info)
        task_name = 'meta.report.roc'
        task_type = 'workflow'
        otu_info = self.meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！"}
            return json.dumps(info)
        task_info = self.meta.get_task_info(otu_info['task_id'])
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'top_n_id': data.top_n_id,
            'method_type': data.method_type,
            'submit_location': data.submit_location,
            'task_type': data.task_type
            }
        main_table_name = 'Roc' + \
            '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('roc_id', ObjectId(data.otu_id)),
            #('table_type', 'dist'),
            #('tree_type', 'cluster'),
            ('status', 'start'),
            ('desc', '正在计算'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("level_id", int(data.level_id)),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = self.meta.insert_main_table('sg_roc', mongo_data)
        update_info = {str(main_table_id): 'sg_roc'}
        options = {
            'otu_table': data.otu_id,
            'otu_id': data.otu_id,
            'level': int(data.level_id),
            'method': data.method_type,
            'top_n': data.top_n_id,
            'group_table': data.group_id,
            'group_detail': data.group_detail,
            'update_info': json.dumps(update_info),
            #'params': json.dumps(params_json, sort_keys=True, separators=(',', ':')),
            'roc_id': str(main_table_id)
            }
        #to_file = 'meta.export_otu_table_by_detail(otu_table)'
        to_file = ["meta.export_otu_table_by_detail(otu_table)", "meta.export_group_table_by_detail(group_table)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name="ROC/" + main_table_name,
                            module_type=task_type, to_file=to_file) # modified by hongdongxuan 20170322 在main_table_name前面加上文件输出的文件夹名
        task_info = super(Roc, self).POST()
        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        print(self.return_msg)
        return json.dumps(task_info)
        # return json.dumps({'success': True, 'info': 'shenghe log'})
