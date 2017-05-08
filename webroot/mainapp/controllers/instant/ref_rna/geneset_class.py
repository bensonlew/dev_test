# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
import datetime
from mainapp.controllers.project.ref_rna_controller import RefRnaController
from mbio.api.to_file.ref_rna import *


class GenesetClassAction(RefRnaController):
    def __init__(self):
        super(GenesetClassAction, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        default_argu = ['geneset_id', 'geneset_type', 'submit_location', 'anno_type']
        for argu in default_argu:
            if not hasattr(data, argu):
                info = {'success': False, 'info': '%s参数缺少!' % argu}
                return json.dumps(info)

        task_name = 'ref_rna.report.geneset_class'
        task_type = 'workflow'
        params_json = {
            "submit_location": data.submit_location,
            "task_type": data.task_type,
            "geneset_id": data.geneset_id,
            "anno_type": data.anno_type
        }
        # 判断传入的基因集id是否存在
        geneset_info = {}
        for geneset in data.geneset_id.split(","):
            geneset_info = self.ref_rna.get_main_info(geneset, 'sg_geneset')
            if not geneset_info:
                info = {"success": False, "info": "geneset不存在，请确认参数是否正确！!"}
                return json.dumps(info)
        task_info = self.ref_rna.get_task_info(geneset_info['task_id'])

        if data.anno_type == "go":
            table_name = "Go"
            collection_name = "sg_geneset_go_class"
            to_file = 'ref_rna.export_go_class(geneset_go)'
            option = {"geneset_go": data.geneset_id}
        elif data.anno_type == "cog":
            table_name = "Cog"
            collection_name = "sg_geneset_cog_class"
            to_file = 'ref_rna.export_cog_class(geneset_cog)'
            option = {"geneset_cog": data.geneset_id}
        elif data.anno_type == "kegg":
            table_name = "Kegg"
            collection_name = "sg_geneset_kegg_class"
            to_file = 'ref_rna.export_multi_gene_list(geneset_kegg)'
            option = {"geneset_kegg": data.geneset_id}
        else:
            info = {'success': False, 'info': '不支持的功能分类!'}
            return json.dumps(info)

        main_table_name = 'Geneset' + table_name + "Class_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]

        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('status', 'start'),
            ('name', main_table_name),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':')))
        ]
        main_table_id = self.ref_rna.insert_main_table(collection_name, mongo_data)
        update_info = {str(main_table_id): collection_name}

        options = {
            "submit_location": data.submit_location,
            "task_type": data.task_type,
            'update_info': json.dumps(update_info),
            "main_table_id": str(main_table_id),
            "geneset_id": data.geneset_id,
            "geneset_type": data.geneset_type,
            "anno_type": data.anno_type,
            }
        options.update(option)
        # print(options)
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name, module_type=task_type,
                            to_file=to_file, project_sn=task_info['project_sn'], task_id=task_info['task_id'])

        task_info = super(GenesetClassAction, self).POST()

        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
                }}

        return json.dumps(task_info)
