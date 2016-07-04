# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import json
import datetime
import re
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.pan_core import PanCore as P
from mainapp.libs.param_pack import param_pack, GetUploadInfo


class PanCoreTest2(MetaController):
    def POST(self):
        myReturn = super(PanCoreTest2, self).POST()
        if myReturn:
            return myReturn
        data = web.input()
        self.client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        param_list = ["group_id", "category_name", "otu_id", "level_id", "submit_location"]
        for my_p in param_list:
            if not hasattr(data, my_p):
                info = {"success": False, "info": "缺少参数{}!".format(my_p)}
                return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['group_id'] = data.group_id
        c_name = re.split(',', data.category_name)
        c_name.sort()
        new_cname = ','.join(c_name)
        my_param['category_name'] = new_cname
        my_param["submit_location"] = data.submit_location
        params = param_pack(my_param)
        
        (output_dir, update_api) = GetUploadInfo(self.client, self.memberId, self.projectSn, self.taskId, "pan_core")
        name = "pan_table_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        pan_id = P().create_pan_core_table(1, params, data.group_id, data.level_id, data.otu_id, name)
        name = "core_table" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        core_id = P().create_pan_core_table(2, params, data.group_id, data.level_id, data.otu_id, name)
        update_info = {str(pan_id): "sg_otu_pan_core", str(core_id): "sg_otu_pan_core"}
        # 字典  id: 表名
        update_info = json.dumps(update_info)
        json_data = {
            "id": self._id,
            "stage_id": 0,
            "name": "meta.report.pan_core",  # src/mbio/meta/report/pan_core
            "type": "workflow",
            "client": self.client,
            "project_sn": self.projectSn,
            "to_file": ["meta.export_otu_table_by_level(in_otu_table)", "meta.export_group_table(group_table)"],
            # src/mbio/api/to_file/meta 括号内的值与options里面的值对应
            # "USE_DB": True,
            # "IMPORT_REPORT_DATA": True,
            "UPDATE_STATUS_API": update_api,  # src/mbio/api/web/update_status
            "output": output_dir,
            "options": {
                "update_info": update_info,
                "in_otu_table": data.otu_id,
                "group_table": data.group_id,
                "category_name": data.category_name,
                "level": data.level_id,
                "pan_id": str(pan_id),
                "core_id": str(core_id)
            }
        }
        self.importWorkflow("mbio.workflows.meta.report.pan_core_test", json.dumps(json_data))
        self.run()
        return self.returnInfo

    def run(self):
        super(PanCoreTest2, self).run()
        self.end()

    def end(self):
        self.uploadFiles("pan_core")
        super(PanCoreTest2, self).end()
