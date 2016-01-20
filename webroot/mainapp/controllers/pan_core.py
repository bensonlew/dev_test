# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
import random


class PanCore(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "otu_id") and hasattr(data, "level")):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)

        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.pan_core",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": "meta.export_otu_table(otu_id)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.otu",
                }

