# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.estimator import Estimator
import random
import datetime
from mainapp.libs.param_pack import GetUploadInfo


class Rarefaction(object):
    """

    """
    ESTIMATORS = ['ace', 'bootstrap', 'chao', 'coverage', 'default', 'heip', 'invsimpson', 'jack', 'npshannon',
                  'shannon', 'shannoneven', 'simpson', 'simpsoneven', 'smithwilson', 'sobs']

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        params_name = ['otu_id', 'level_id', 'index_type', 'freq', 'submit_location']
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!" % param}
                return json.dumps(info)
        for index in data.index_type.split(','):
            if index not in self.ESTIMATORS:
                info = {"success": False, "info": "指数类型不正确{}".format(index)}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {"success": False, "info": "level{}不在规定范围内{}".format(data.level_id)}
            return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = int(data.level_id)
        # my_param['indices'] = data.index_type
        my_param['freq'] = data.freq
        sort_index = data.index_type.split(',')
        sort_index.sort()
        sort_index = ','.join(sort_index)
        my_param['index_type'] = sort_index
        my_param['submit_location'] = data.submit_location
        my_param['task_type'] = data.task_type
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))

        otu_info = Meta().get_otu_table_info(data.otu_id)

        if otu_info:
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            pre_path = "sanger:rerewrweset/files/" + str(member_id) + "/" + str(otu_info["project_sn"]) + "/" + \
                       str(otu_info['task_id']) + "/report_results/"
            name = "Rarefaction_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            rare_id = Estimator().add_rare_collection(data.level_id, params, data.otu_id, name)
            # print(rare_id)
            update_info = {str(rare_id): "sg_alpha_rarefaction_curve"}
            update_info = json.dumps(update_info)

            (output_dir, update_api) = GetUploadInfo(client, member_id, otu_info["project_sn"],
                                                     otu_info['task_id'], "rarefaction")
            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            # print(workflow_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.rarefaction",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": "meta.export_otu_table_by_level(otu_table)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options": {
                    "update_info": update_info,
                    "otu_id": data.otu_id,
                    "otu_table": data.otu_id,
                    "indices": data.index_type,
                    "level": data.level_id,
                    "freq": data.freq,
                    "rare_id": str(rare_id)
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            # return json.dumps(json_obj)
            info = {"success": True, "info": "提交成功!"}
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
