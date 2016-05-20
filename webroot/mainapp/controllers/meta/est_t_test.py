# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
import random
import datetime
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.libs.param_pack import group_detail_sort
from mainapp.models.mongo.estimator import Estimator
from mainapp.models.mongo.meta import Meta


class EstTTest(object):
    """

    """
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        params_name = ['alpha_diversity_id', 'group_detail', 'group_id', 'submit_location']
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!" % param}
                return json.dumps(info)
        table_dict = json.loads(data.group_detail)
        if not isinstance(table_dict, dict):
            info = {"success": False, "info": "传入的group_detail不是字典"}
            return json.dumps(info)
        if len(table_dict) < 2:
            info = {"success": False, "info": "请选择至少两组及以上的分组"}
            return json.dumps(info)
        my_param = dict()
        my_param['alpha_diversity_id'] = data.alpha_diversity_id
        my_param['group_detail'] = group_detail_sort(data.group_detail)
        my_param['group_id'] = data.group_id
        my_param['submit_location'] = data.submit_location
        est_params = Estimator().get_est_params(data.alpha_diversity_id)
        my_param['otu_id'] = str(est_params[0])
        # print(my_param)
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        # print(est_info)
        otu_info = Meta().get_otu_table_info(est_params[0])
        if otu_info:
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            pre_path = "sanger:rerewrweset/files/" + str(member_id) + "/" + str(otu_info["project_sn"]) + "/" + \
                       str(otu_info['task_id']) + "/report_results/"
        else:
            info = {"success": False, "info": "指数表对应的OTU表不存在，无法找到member_id，请确认参数是否正确！!"}
            return json.dumps(info)
        est_info = Estimator().get_est_table_info(data.alpha_diversity_id)
        if est_info:
            name = "est_t_test_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            est_t_test_id = Estimator().add_est_t_test_collection(params, data.group_id, data.alpha_diversity_id, name)
            # print(est_t_test_id)
            update_info = {str(est_t_test_id): "sg_alpha_est_t_test", str(est_t_test_id): "sg_alpha_est_t_test"}
            update_info = json.dumps(update_info)

            workflow_id = self.get_new_id(est_info["task_id"], data.alpha_diversity_id)
            # print(workflow_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.est_t_test",
                "type": "workflow",
                "client": client,
                "project_sn": est_info["project_sn"],
                "to_file": ["estimator.export_est_table(est_table)", "meta.export_group_table_by_detail(group_table)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "IMPORT_REPORT_AFTER_END": False,
                "output":  pre_path + name,
                "options": {
                    "update_info": update_info,
                    "est_table": data.alpha_diversity_id,
                    "group_detail": data.group_detail,
                    "group_table": data.group_id,
                    # "test_type": 'student',
                    "est_t_test_id": str(est_t_test_id)
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
            info = {"success": False, "info": "指数表不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
