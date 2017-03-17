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
        params_name = ['father_id', 'err_min', 'dedup','submit_location']
        for param in params_name:
            if not hasattr(data, param):
                info = {'success': False, 'info': '缺少{}参数'.format(param)}
                return json.dumps(info)
        father_info = PT().get_query_info(data.father_id)
        ref_info = PT().get_ref_info(data.father_id)
        if not father_info:
            info = {'success': False, 'info': 'father_id不存在'}
            return json.dumps(info)
        task_name = 'paternity_test.report.pt_report'
        task_type = 'workflow'
        params_json = {
            'err_min': int(data.err_min),
            'dedup': int(data.dedup),
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        params = json.dumps(params_json, sort_keys=True, separators=(',', ':'))
        mongo_data = [
            ('params', params),
            ('father_id', ObjectId(data.father_id)),
            ('name', 'err-' + str(data.err_min) + '_dedup-' + str(data.dedup)),
            ("created_ts", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        main_table_id = PT().insert_main_table('sg_pt_father', mongo_data)
        update_info = {str(main_table_id): 'sg_report_flow'}
        update_info = json.dumps(update_info)
        options = {
            "ref_fasta": str(ref_info['ref_fasta']),
            "targets_bedfile": str(ref_info['targets_bedfile']),

            "dad_id": str(father_info['dad_id']),
            "mom_id": father_info['mom_id'],
            "preg_id": father_info['preg_id'],
            "ref_point": str(ref_info['ref_point']),

            "err_min": int(data.err_min),
            "dedup_num": int(data.dedup),
            "pt_father_id": str(main_table_id),
            "update_info": update_info,
        }
        print options
        self.set_sheet_data(name=task_name, options=options,module_type=task_type, params=params)
        task_info = super(PaternityTestNew, self).POST()
        return json.dumps(task_info)

# python /mnt/ilustre/users/sanger-dev/biocluster/bin/webapitest.py post paternity_test_new -c client03 -b http://192.168.12.102:9091 -n "err_min;dedup;father_id;submit_location" -d "3;50;58ca46b9a4e1af6b57c5fd64;XXX"