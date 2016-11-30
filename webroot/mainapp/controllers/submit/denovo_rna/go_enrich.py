# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modify:20161121

import web
import json
import random
import types
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from biocluster.config import Config
from mbio.api.database.denovo_express import *
from mainapp.models.mongo.submit.denovo_rna.denovo_go_enrich import DenovoGoEnrich
from mainapp.models.mongo.denovo import Denovo
from mainapp.models.workflow import Workflow


class GoEnrich(object):
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, 'client') else web.ctx.env.get('HTTP_CLIENT')
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param['enrich_id'] = data.enrich_id
        my_param['annotation_id'] = data.annotation_id
        my_param['pval'] = data.pval
        my_param['method'] =data.method
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        print data.go_enrich_id
        go_enrich_info = Denovo().get_main_info(data.enrich_id, 'sg_denovo_go_enrich')  # 找到集合sg_denovo_go_enrich的主表id对应的go_enrich_id
        if go_enrich_info:
            task_id = go_enrich_info['task_id']
            project_sn = go_enrich_info['project_sn']
            task_info = Denovo().get_task_info(task_id)  # 找到集合sg_task，task_id
            if task_info:
                member_id = task_info['member_id']
            else:
                info = {"success": False, "info": "这个go_enrich_id对应的task:{}没有member_id".format(go_enrich_info['task_id'])}
                return json.dumps(info)
            go_enrich_id = DenovoGoEnrich().add_go_enrich(name=None, params=params, project_sn=project_sn, task_id=task_id, go_graph_dir=None, go_enrich_dir=None)
            update_info = {str(go_enrich_id): "sg_denovo_go_enrich_detail", "database": self.db_name}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(task_id, data.enrich_id)
            (output_dir, update_api) = GetUploadInfo_denovo(client, member_id, project_sn, task_id, 'go_enrich_stat')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "denovo_rna.report.go_enrich",
                "type": "workflow",
                "client": client,
                "project_sn": project_sn,
                "to_file": ["denovo. go_enrich(enrich_file)", "denovo.go_enrich_annotation(gos_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATA_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "option": {
                    "enrich_file": data.enrich_id,
                    "gos_file": data.annotation_id,
                    "pval": data.pval,
                    "method": data.method
                }
            }
            insert_data = {
                "client": client,
                "workflow_id": workflow_id,
                "json": json.dumps(json_data),
                "ip": web.ctx.ip
            }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功！"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "enrich_id不存在，请确认参数是否正确！"}
            return json.dumps(info)

    def get_new_id(self, task_id, main_id):
        new_id = "%s_%s_%s" % (task_id, main_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, main_id)
        return new_id

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['enrich_id', 'annotation_id', 'pval', 'method']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数！")
        for ids in [data.enrich_id, data.annotation_id, data.pval, data.method]:
            ids = str(ids)
            print type(ids)
            if not isinstance(ids, ObjectId) and not isinstance(ids, types.StringTypes):
                success.append("传入的id:{}不是一个ObejectId对象或字符串类型".format(ids))
        return success
