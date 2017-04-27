# -*- coding: utf-8 -*-
# __author__ = 'hongdongxuan'
import web
import json
import datetime
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from bson.objectid import ObjectId


class PpinetworkAction(RefRnaController):
    def __init__(self):
        super(PpinetworkAction, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        default_argu = ['geneset_id', 'species', 'submit_location', 'combine_score', 'task_type']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)

        task_name = 'ref_rna.report.ppinetwork'
        task_type = 'workflow'
        params_json = {
            "submit_location": data.submit_location,
            "task_type": data.task_type,
            "geneset_id": data.geneset_id,
            "species": data.species,
            "combine_score": data.combine_score
        }
        geneset_info = self.ref_rna.get_main_info(data.geneset_id, 'sg_geneset')
        if not geneset_info:
            info = {"success": False, "info": "geneset不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_info = self.ref_rna.get_task_info(geneset_info['task_id'])
        main_table_name = 'PPINetwork_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]

        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('status', 'start'),
            ('name', main_table_name),
            ('geneset_id', ObjectId(data.geneset_id)),
            ('desc', 'ppi_network分析中...'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = self.ref_rna.insert_main_table('sg_ppinetwork', mongo_data)
        update_info = {str(main_table_id): "sg_ppinetwork"}

        options = {
            'update_info': json.dumps(update_info),
            "ppi_id": str(main_table_id),
            "geneset_id": data.geneset_id,
            "diff_exp_gene": data.geneset_id,
            "species": data.species,
            "combine_score": data.combine_score,
        }
        to_file = "ref_rna.export_gene_list_ppi(diff_exp_gene)"
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name, module_type=task_type,
                            to_file=to_file, project_sn=task_info['project_sn'], task_id=task_info['task_id'])

        task_info = super(PpinetworkAction, self).POST()

        task_info['content'] = {'ids': {'id': str(main_table_id), 'name': main_table_name}}
        return json.dumps(task_info)
