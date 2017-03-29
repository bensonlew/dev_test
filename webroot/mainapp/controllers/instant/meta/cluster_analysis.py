# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import param_pack, group_detail_sort
from mainapp.models.mongo.meta import Meta   # 20170106 2 lines
from bson import ObjectId


class ClusterAnalysis(MetaController):
    def __init__(self):  # 20170106 2 lines
        super(ClusterAnalysis, self).__init__(instant=True)

    def POST(self):
        # return_info = super(ClusterAnalysis, self).POST() # 20170106 3 lines
        # if return_info:
        #     return return_info
        data = web.input()
        postArgs = ["group_detail", "otu_id", "level_id", "task_type", "group_id"]  # modify by zhouxuan 2016.11.22
        for arg in postArgs:
            if not hasattr(data, arg):
                info = {'success': False, 'info': '{}参数缺少!'.format(arg)}
                return json.dumps(info)
        task_name = 'meta.report.cluster_analysis'
        meta = Meta()  # 20170106 6 lines
        otu_info = meta.get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = meta.get_task_info(otu_info['task_id'])
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'submit_location': data.submit_location,
            'task_type': data.task_type
        }
        main_table_name = 'CommunityBarPie_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  #modified by hongdongxuan 201703221 将ClusteringAnalysis_改为CommunityBarPie
        newick_id = None
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('from_id', data.otu_id),  # maybe ObjectId(data.otu_id)
            ('name', main_table_name),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':'))),
            ('newick_id', newick_id),
            ('status', 'start'),
            ('desc', 'otu table after Cluster Analysis'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("show", 0),
            ("type", "otu_group_analyse")
        ]
        main_table_id = meta.insert_main_table('sg_otu', mongo_data)
        update_info = {str(main_table_id): 'sg_otu'}
        options = {
            "input_otu_id": data.otu_id,
            "in_otu_table": data.otu_id,
            "group_detail": data.group_detail,
            "level": str(data.level_id),
            'update_info': json.dumps(update_info),
            'main_id': str(main_table_id)
        }
        to_file = "meta.export_otu_table_by_level(in_otu_table)"
        self.set_sheet_data(name=task_name, options=options, main_table_name="CommunityAnalysis/" + main_table_name,
                            module_type='workflow', to_file=to_file)
        task_info = super(ClusterAnalysis, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }}
        return json.dumps(task_info)