# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modify:20161201
import web
import json
import random
import types
from mainapp.libs.signature import check_sig
from mainapp.libs.param_pack import GetUploadInfo_denovo
from biocluster.config import Config
from mbio.api.database.denovo_express import *
from mainapp.models.mongo.submit.denovo_rna.denovo_go_enrich import DenovoEnrich
from mainapp.models.mongo.denovo import Denovo
from mainapp.models.workflow import Workflow


class GoEnrichRegulate(object):
    """
    go富集分析、go调控分析接口
    """
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        print data
        if not hasattr(data, "analysis_type"):
            info = {"success": False, "info": "缺少参数analysis_type!"}
            return json.dumps(info)
        if not hasattr(data, "express_id"):
            info = {"success": False, "info": "缺少参数express_id!"}
            return json.dumps(info)
        analysis_type = data.analysis_type
        if analysis_type not in ["enrich", "regulate", "stat"]:
            info = {"success": False, "info": "{}分析不存在".format(analysis_type)}
            return json.dumps(info)
        if analysis_type in ["enrich", "stat"]:
            for param in ["pval", "method"]:
                if not hasattr(data, param):
                    info = {"success": False, "info": "缺少{}参数".format(param)}
        express_info = Denovo().get_main_info(data.express_id, "sg_denovo_express")
        if express_info:
            task_info = Denovo().get_task_info(express_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_id对应的task:{}没有member_id".format(express_info["task_id"])}
                return json.dumps(info)
            insert_data = self.get_insert_data(analysis_type, client, express_info, data, member_id)
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "表达量表id不存在！!"}
            return json.dumps(info)

    def get_params(self, data):
        my_param = {'analysis_type': data.analysis_type, "express_id": data.express_id}
        if data.analysis_type in ["enrich", "stat"]:
            my_param["pval"] = data.pval
            my_param["method"] = data.method
        return my_param

    def get_insert_data(self, analysis_type, client, express_info, data, member_id):
        my_params = self.get_params(data)
        params = my_params   #
        options = {"analysis_type": data.analysis_type, "method": data.method} #express_id
        project_sn = express_info["project_sn"]
        task_id = express_info["task_id"]
        if analysis_type == "regulate":
            go_regulate_id = DenovoEnrich().add_go_regulate()
            update_info = {str(go_regulate_id): "sg_denovo_regulate", "database": self.db_name}
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["go_regulate_id"] = str(go_regulate_id)
            options["regulate_file"] = data.express_id
            options.update(my_params)
            to_file = ["denovo.go_regulate(regulate_file)"]
        elif analysis_type == "enrich":
            go_enrich_id = DenovoEnrich().add_go_enrich()
            update_info = {str(go_enrich_id): "sg_denovo_enrich", "database": self.db_name}
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["go_enrich_id"] = str(go_enrich_id)
            options["pval"] = data.pval
            options["method"] = data.method
            options["enrich_file"] = data.express_id
            options.update(my_params)
            to_file = ["denovo.go_enrich(enrich_file)"]
        elif analysis_type == "stat":
            go_regulate_id = DenovoEnrich().add_go_regulate()
            go_enrich_id = DenovoEnrich().add_go_enrich()
            update_info = [{str(go_regulate_id): "sg_denovo_regulate", "database": self.db_name}, {str(go_enrich_id): "sg_denovo_enrich", "database": self.db_name}]
            update_info = json.dumps(update_info)
            options["update_info"] = update_info
            options["go_enrich_id"] = str(go_enrich_id)
            options["go_regulate_id"] = str(go_regulate_id)
            options["pval"] = data.pval
            options["method"] = data.method
            options["enrich_file"] = data.express_id
            options["regulate_file"] = data.express_id
            options.update(my_params)
            to_file = ["denovo.go_enrich(enrich_file)", "denovo.go_regulate(regulate_file)"]
        workflow_id = self.get_new_id(express_info["task_id"], data.express_id)
        (output_dir, update_api) = GetUploadInfo_denovo(client, member_id, project_sn, task_id, 'go_enrich_regulate')
        json_data = {
            "id": workflow_id,
            "stage_id": 0,
            "name": "denovo_rna.report.go_enrich_regulate",
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
