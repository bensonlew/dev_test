# -*- coding: utf-8 -*-
# __author__ = 'shijin'

import web
import json
import types
import datetime
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from biocluster.config import Config
from mainapp.models.mongo.submit.ref_rna.RefrnaKeggEnrich import RefrnaEnrich
from mainapp.models.mongo.meta import Meta
from mainapp.models.workflow import Workflow
from mainapp.controllers.project.ref_rna_controller import RefRnaController


class KeggEnrich(RefRnaController):
    """
    kegg富集、调控接口
    """
    def __init__(self):
        super(KeggEnrich, self).__init__(instant=False)

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": json.dumps(return_result)}
            return json.dumps(info)
        my_param = {'gene_set': data.gene_set, "correct": data.correct, "submit_location": data.submit_location}
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        # name = data.compare.split(',')[0]
        # compare_name = data.compare.split(',')[1]
        geneset_info = Meta(db=self.mongodb).get_main_info(data.gene_set, "sg_geneset")
        name = geneset_info["name"]  # 从geneset表中确定name，根据name确定background
        # 读取到geneset主表中的一条记录
        if not geneset_info:
            info = {"success": False, "info": "geneset_info不存在，请检查参数是否正确！"}
            return json.dumps(info)
        task_id = geneset_info["task_id"]
        project_sn = geneset_info["project_sn"]
        main_table_name = "KeggEnrich_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        kegg_enrich_id = RefrnaEnrich().add_kegg_rich(name=main_table_name, params=params, project_sn=project_sn, task_id=task_id)
        update_info = {str(kegg_enrich_id): "sg_geneset_kegg_enrich"}
        options = {
            "analysis_type": "enrich",
            "update_info": json.dumps(update_info),
            "name": name,
            "compare_name": "",
            "kegg_enrich_id": str(kegg_enrich_id),
            "kegg_table": data.gene_set,
            "diff_stat": data.gene_set,
            "all_list" : data.gene_set,
            "correct": data.correct
        }
        to_file = ["ref_rna.export_diff_express(diff_stat)", "ref_rna.export_all_gene_list(all_list)",
                   "ref_rna.export_kegg_table(kegg_table)"]  # 需要编写
        self.set_sheet_data(name="denovo_rna.report.kegg_rich_regulate", options=options,
                            main_table_name=main_table_name, module_type="workflow", to_file=to_file,
                            main_id=kegg_enrich_id, collection_name="sg_geneset_kegg_enrich")
                            # 使用denovo的重运行workflow
        task_info = super(KeggEnrich, self).POST()
        task_info['content'] = {'ids': {'id': str(kegg_enrich_id), 'name': main_table_name}}
        return json.dumps(task_info)

    def check_options(self, data):
        """
        检查网页端传来的参数是否正确
        """
        params_name = ["gene_set", "correct"]
        success = []
        for name in params_name:
            if not hasattr(data, name):
                success.append("缺少参数：%" % name)
        gene_set_id = str(data.gene_set)
        if not isinstance(gene_set_id, ObjectId) and not isinstance(gene_set_id, types.StringType):
            success.append("传入的gene_set_id:%不是一个ObjectId对象或字符串类型!" % gene_set_id)
        correct = data.correct
        if correct not in ["bonferroni", "sidak", "holm", "fdr"]:
            success.append("%分析不存在" % correct)
        return success
