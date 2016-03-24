# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import re
import json
import random
import datetime
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.libs.param_pack import param_pack
from mainapp.models.mongo.heat_cluster import HeatCluster as H


class HeatCluster(object):
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        param_list = ["samples", "otu_id", "level_id", "linkage"]
        for my_p in param_list:
            if not hasattr(data, my_p):
                info = {"success": False, "info": "缺少参数{}!".format(my_p)}
                return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['linkage'] = data.linkage
        my_sp = re.split(',', data.samples)
        my_sp.sort()
        my_param['samples'] = ",".join(my_sp)
        params = param_pack(my_param)
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "heat_cluster_" + str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))
            newick_id = H().create_newick_table(params, data.linkage, data.otu_id, name)
            update_info = {str(newick_id): "sg_newick_tree"}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.heat_cluster",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": ["meta.export_otu_table_by_level(in_otu_table)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": update_info,
                    "in_otu_table": data.otu_id,
                    "samples": data.samples,
                    "linkage": data.linkage,
                    "level": data.level_id,
                    "newick_id": str(newick_id)
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
            info = {"success": True, "info": "提交成功!正在计算聚类树..."}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
