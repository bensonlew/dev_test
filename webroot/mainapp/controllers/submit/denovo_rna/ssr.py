# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from biocluster.config import Config
from mbio.api.database.denovo_express import *
from mainapp.models.mongo.submit.denovo_rna.gene_structure import GeneStructure
import types
from mainapp.models.mongo.denovo import Denovo
from mainapp.models.workflow import Workflow


class Ssr(object):
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "sequence_id"):
            info = {"success": False, "info": "缺少参数sequence_id!"}
            return json.dumps(info)
        if not hasattr(data, "orf_id"):
            info = {"success": False, "info": "缺少参数orf_id!"}
            return json.dumps(info)
        if not hasattr(data, "primer"):
            info = {"success": False, "info": "缺少参数primer!"}
            return json.dumps(info)
        print(data.primer)
        # if data.primer == "0":
        #     primer = "true"
        # else:
        #     primer = "false"
        name = "ssr_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        params = {"sequence_id": data.sequence_id, "primer": data.primer, "orf_id": data.orf_id, "task_type": data.task_type, "submit_location": data.submit_location}
        sequence_info = Denovo().get_main_info(data.sequence_id, 'sg_denovo_sequence')
        if sequence_info:
            task_id = sequence_info["task_id"]
            project_sn = sequence_info["project_sn"]
            task_info = Denovo().get_task_info(task_id)
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个sequence_info对应的序列信息对应的task：{}没有member_id!".format(sequence_info["task_id"])}
                return json.dumps(info)
            ssr_id = GeneStructure().add_ssr_table(project_sn, task_id, params=params, name=name)
            update_info = {str(ssr_id): "sg_denovo_ssr", 'database': self.db_name}
            update_info = json.dumps(update_info)
            workflow_id = Denovo().get_new_id(task_id, data.sequence_id)
            (output_dir, update_api) = GetUploadInfo_test(client, member_id, project_sn, task_id, 'ssr')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "denovo_rna.report.ssr",
                "type": "workflow",
                "client": client,
                "project_sn": project_sn,
                "to_file": ["denovo.export_fasta_path(gene_fasta)", "denovo.export_bed_path(bed_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "gene_fasta": data.sequence_id,
                    "insert_id": str(ssr_id),
                    "bed_file": data.orf_id,
                    "update_info": update_info,
                    "primer": data.primer
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "sequence_id不存在，请确认参数是否正确！!"}
            return json.dumps(info)
