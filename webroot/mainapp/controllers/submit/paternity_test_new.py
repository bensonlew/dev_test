# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import web
import json
import datetime
from mainapp.models.mongo.submit.paternity_test_mongo import PaternityTest as PT
from mainapp.controllers.project.pt_controller import PtController
from mainapp.libs.param_pack import *
from bson import ObjectId


class PaternityTestNew(PtController):

    def __init__(self):
        super(PaternityTestNew, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        params_name = ['task_id', 'err_min', 'dedup']
        for param in params_name:
            if not hasattr(data, param):
                info = {'success': False, 'info': '缺少{}参数'.format(param)}
                return json.dumps(info)
        task_info = PT().get_query_info(data.task_id)
        print task_info
        if not task_info:
            info = {'success': False, 'info': 'task_id不存在'}
            return json.dumps(info)
        task_name = 'paternity_test.report.pt_report'
        # task_type = 'workflow'
        params_json = {
            'err_min': int(data.err_min),
            'dedup': int(data.dedup)
        }
        params = json.dumps(params_json, sort_keys=True, separators=(',', ':'))
        mongo_data = [
            ('params', params),
            ('task_id', data.task_id),
            ('name', 'err-' + str(data.err_min) + '_dedup-' + str(data.dedup)),
            ("created_ts", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        main_table_id = PT().insert_main_table('sg_pt_family', mongo_data)
        update_info = {str(main_table_id): 'sg_report_flow'}
        update_info = json.dumps(update_info)
        options = {
            "ref_fasta": str(task_info['ref_fasta']),
            "targets_bedfile": str(task_info['targets_bedfile']),

            "dad_id": str(task_info['dad_id']),
            "mom_id": task_info['mom_id'],
            "preg_id": task_info['preg_id'],
            "ref_point": str(task_info['ref_point']),

            "err_min": int(data.err_min),
            "dedup_num": int(data.dedup),
            "family_id": str(main_table_id),
            "update_info": update_info,
        }
        self.set_sheet_data(name=task_name, options=options,module_type='workflow', params=params)
        task_info = super(PaternityTestNew, self).POST()
        return json.dumps(task_info)

# python /mnt/ilustre/users/sanger-dev/biocluster/bin/webapitest.py post paternity_test_new -c client03 -b http://192.168.12.102:9092 -n "err_min;dedup;task_id" -d "3;50;sg_6027"