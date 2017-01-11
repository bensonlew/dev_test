# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort
from mainapp.models.mongo.meta import Meta


class OtuSubsample(MetaController):
    def __init__(self):
        super(OtuSubsample, self).__init__(instant=True)

    def POST(self):
        #return_info = super(OtuSubsample, self).POST()
        #if return_info:
         #   return return_info
        data = web.input()
        postArgs = ['size', 'submit_location', "otu_id", "task_type", "group_detail", "group_id", "filter_json"]
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '%s参数缺少!' % arg}
                return json.dumps(info)
        
        meta = Meta()
        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        task_type = 'workflow'
        main_table_name = 'OTU_taxon_analysis' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        my_param = dict()
        my_param["group_id"] = data.group_id
        my_param['otu_id'] = data.otu_id
        my_param["submit_location"] = data.submit_location
        my_param["size"] = data.size
        my_param["filter_json"] = json.loads(data.filter_json)
        my_param["group_detail"] = group_detail_sort(data.group_detail)
        my_param["task_type"] = data.task_type
        params = param_pack(my_param)
        #self.run()
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('from_id', str(data.otu_id)),
            ('name', main_table_name),
            ("params", params),
            ('status', 'start'),
            ("level_id", json.dumps([9])),
            ('desc', '正在计算'),
            ("type", "otu_statistic"),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]

        main_table_id = meta.insert_main_table('sg_otu', mongo_data)
        update_info = {str(main_table_id): 'sg_otu'}
        options = {
            "in_otu_table": data.otu_id,
            "input_otu_id": data.otu_id,
            'group_id': data.group_id,
            "group_detail": data.group_detail,
            "filter_json": data.filter_json,
            "level": "9",
            "size": data.size,
            'update_info': json.dumps(update_info),
            "params": params,
            'main_id': str(main_table_id)
        }

        to_file = "meta.export_otu_table_by_level(in_otu_table)"
        task_name = 'meta.report.otu_subsample'
        self.set_sheet_data(name = task_name, options = options, main_table_name= main_table_name, module_type= task_type, to_file= to_file)
        task_info = super(OtuSubsample, self).POST()
        task_info['content'] = {'ids':{'id':str(main_table_id),'name':main_table_name}}
        return json.dumps(task_info)