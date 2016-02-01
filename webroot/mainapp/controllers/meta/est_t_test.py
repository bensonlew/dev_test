# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import web
import json
import random
import datetime
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
# from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.estimator import Estimator


class EstTTest(object):
    """

    """
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "alpha_diversity_id"):
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        # print(data.alpha_diversity_id)
        my_param = dict()
        my_param['alpha_diversity_id'] = data.alpha_diversity_id
        my_param['category_name'] = data.category_name
        params = json.dumps(my_param)
        est_info = Estimator().get_est_table_info(data.alpha_diversity_id)
        # print(est_info)
        if est_info:
            name = str(datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")) + "_est_t_test"
            est_t_test_id = Estimator().add_est_t_test_collection(params, data.alpha_diversity_id, name)
            print(est_t_test_id)
            update_info = {str(est_t_test_id): "sg_alpha_est_t_test", str(est_t_test_id): "sg_alpha_est_t_test"}
            update_info = json.dumps(update_info)

            workflow_id = self.get_new_id(est_info["task_id"], data.alpha_diversity_id)
            print(workflow_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.est_t_test",
                "type": "workflow",
                "client": client,
                "project_sn": est_info["project_sn"],
                "to_file": ["estimator.export_est_table(est_table)",  "meta.export_group_table_by_detail(group_table)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "options": {
                    "update_info": update_info,
                    "est_table": data.alpha_diversity_id,
                    # "task_id": otu_info["task_id"],
                    "group_table": data.category_name,
                    "test_type": 'student',
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
