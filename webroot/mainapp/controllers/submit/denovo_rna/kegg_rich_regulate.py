# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
import datetime
from biocluster.config import Config
from mainapp.models.mongo.denovo import Denovo
from mainapp.libs.param_pack import GetUploadInfo_denovo
from mbio.api.database.denovo_kegg_rich import *
from mbio.api.database.denovo_kegg_regulate import *
from mbio.api.database.denovo_kegg_pval_sort import *
from mainapp.models.mongo.submit.denovo_rna.denovo_kegg_rich import DenovoKeggRich
import random


class KeggRichRegulate(object):
    """
    kegg富集、调控接口
    """
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        print data
        params_name = ["express_diff_id", "submit_location", "analysis_type", "compare"]
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!" % param}
                return json.dumps(info)
        if data.analysis_type not in ["enrich", "regulate", "both"]:
            info = {"success": False, "info": "$s分析不存在" % data.analysis_type}
            return json.dumps(info)
        express_diff_info = Denovo().get_main_info(data.express_diff_id, "sg_denovo_express_diff")
        if express_diff_info:
            task_info = Denovo().get_task_info(express_diff_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_diff_id对应的task: %s没有member_id!" % express_diff_info["task_id"]}
                return json.dumps(info)
            insert_data = self.get_insert_data(client, data, member_id, express_diff_info)   ###
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "差异表达id不存在!"}
            return json.dumps(info)

    def get_params(self, data):
        my_param = {"analysis_type": data.analysis_type, "express_diff_id": data.express_diff_id, "compare": data.compare, "submit_location": data.submit_location}
        if data.analysis_type in ["enrich", "both"]:
            my_param["correct"] = data.correct
        return my_param

    def get_insert_data(self, client, data, member_id, express_diff_info):
        params = self.get_params(data)
        analysis_type = data.analysis_type
        name = data.compare.split(',')[0]
        compare_name = data.compare.split(',')[1]
        options = {"analysis_type": analysis_type, "name": name, "compare_name": compare_name, "kegg_table": data.express_diff_id, "diff_stat": data.express_diff_id, "submit_location": data.submit_location}
        project_sn = express_diff_info["project_sn"]
        task_id = express_diff_info["task_id"]
        to_file = ["denovo.export_kegg_table(kegg_table)", "denovo.export_diff_express(diff_stat)"]
        if analysis_type in ["enrich", "both"]:
            rich_id = DenovoKeggRich().add_kegg_rich(name=None, params=params, project_sn=project_sn, task_id=task_id)
            update_info = {str(rich_id): "sg_denovo_kegg_enrich", "database": self.db_name}
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["kegg_enrich_id"] = str(rich_id)
            options["all_list"] = data.express_diff_id
            options["correct"] = data.correct
            options.update(params)
            to_file.append("denovo.export_all_gene_list(all_list)")
        if analysis_type in ["regulate", "both"]:
            regulate_id = DenovoKeggRich().add_kegg_regulate(name=None, params=params, project_sn=project_sn, task_id=task_id)
            update_info = {str(regulate_id): "sg_denovo_kegg_regulate", "database": self.db_name}
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["kegg_regulate_id"] = str(regulate_id)
            options.update(params)
        if analysis_type == "both":
            sort_id = DenovoKeggRich().add_kegg_pval_sort(name=None, params=params, project_sn=project_sn, task_id=task_id)
            update_info = [{str(rich_id): "sg_denovo_kegg_enrich", "database": self.db_name}, {str(regulate_id): "sg_denovo_kegg_regulate", "database": self.db_name}, {str(sort_id): "sg_denovo_kegg_pval_sort", "database": self.db_name}]
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["sort_id"] = str(sort_id)
            options.update(params)
        workflow_id = self.get_new_id(express_diff_info["task_id"], data.express_diff_id)
        (output_dir, update_api) = GetUploadInfo_denovo(client, member_id, project_sn, task_id, 'kegg_enrich_regulate')
        json_data = {
            "id": workflow_id,
            "stage_id": 0,
            "name": "denovo_rna.report.kegg_rich_regulate",
            "type": "workflow",
            "client": client,
            "project_sn": project_sn,
            "to_file": to_file,
            "USE_DB": True,
            "IMPORT_REPORT_DATA": True,
            "UPDATA_STATUS_API": update_api,
            "IMPORT_REPORT_AFTER_END": True,
            "output": output_dir,
            "options": options
        }
        insert_data = {
            "client": client,
            "workflow_id": workflow_id,
            "json": json.dumps(json_data),
            "ip": web.ctx.ip
        }
        print options
        return insert_data

    def get_new_id(self, task_id, main_id):
        new_id = "%s_%s_%s" % (task_id, main_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, main_id)
        return new_id
