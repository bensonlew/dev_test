# -*- coding: utf-8 -*-
# __author__ = 'hesheng'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.controllers.core.basic import Basic
from biocluster.core.function import filter_error_info
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.ref_rna import RefRna
from biocluster.config import Config
from biocluster.wpm.client import *
import random
import datetime


class DemoMongodataCopy(object):
    @check_sig
    def POST(self):
        data = web.input()
        requires = ['type', 'task_id', 'target_task_id', 'target_project_sn', 'target_member_id']
        for i in requires:
            if not (hasattr(data, i)):
                return json.dumps({"success": False, "info": "缺少%s参数!" % i})
        workflow_id = self.get_new_id(data.task_id)
        task_id1 = data.task_id
        if data.type == 'meta':
            data_json = {
                'id': workflow_id,
                'stage_id': 0,
                'name': "copy_demo.copy_demo",  # 需要配置
                'type': 'workflow',  # 可以配置
                'client': data.client,
                'project_sn': data.target_project_sn,
                'options': {
                    "task_id": data.task_id,
                    "target_task_id": data.target_task_id,
                    "target_project_sn": data.target_project_sn,
                    "target_member_id": data.target_member_id
                }
            }
        elif data.type == "ref_rna":
            data_json = {
                'id': workflow_id,
                'stage_id': 0,
                'name': "copy_demo.refrna_copy_demo",  # 需要配置
                'type': 'workflow',  # 可以配置
                'client': data.client,
                'project_sn': data.target_project_sn,
                'options': {
                    "task_id": data.task_id,
                    "target_task_id": data.target_task_id,
                    "target_project_sn": data.target_project_sn,
                    "target_member_id": data.target_member_id
                }
            }
            mongodb = Config().mongo_client[Config().MONGODB + "_ref_rna"]
            collection = mongodb['sg_task']
            nums1 = collection.count({"task_id": {"$regex": data.task_id}})
            nums = collection.count({"task_id": {"$regex": data.task_id}, "demo_status": "end"})
            if nums1:
                if nums1 <= 3:
                    id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
                    json_obj = {
                        "type": "workflow",
                        "id": id,
                        "name": "copy_demo.demo_backup",
                        "IMPORT_REPORT_DATA": True,
                        "IMPORT_REPORT_AFTER_END": False,
                        "options": {
                            "task_id": data.task_id,
                            "target_task_id": data.task_id + '_' + id,
                            "target_project_sn": "refrna_demo",
                            "target_member_id": "refrna_demo"
                        }
                    }
                    worker = worker_client()
                    worker.add_task(json_obj)
                if nums < 3:
                    info = {"success": False, "info": "demo数据正在准备中，请一段时间后再次进行拉取"}
                    return json.dumps(info)
            else:
                info = {"success": False, "info": "demo数据有问题，不能进行拉取"}
                return json.dumps(info)  # add by hongdongxuan 20171022
        else:
            info = {"success": False, "info": "项目类型type：{}不合法!".format(data.type)}
            return json.dumps(info)  # 85-86 add by hongdongxuan 20171022
        workflow_client = Basic(data=data_json, instant=True)
        try:
            run_info = workflow_client.run()
            run_info['info'] = filter_error_info(run_info['info'])
            return json.dumps(run_info)
        except Exception as e:
            return json.dumps({"success": False, "info": "运行出错: %s" % filter_error_info(str(e))})

    def get_new_id(self, task_id, otu_id=None):
        """
        根据旧的ID生成新的workflowID，固定为旧的后面用“_”，添加两次随机数或者一次otu_id一次随机数
        """
        if otu_id:
            new_id = "{}_{}_{}".format(task_id, otu_id[-4:], random.randint(1, 10000))
        else:
            new_id = "{}_{}_{}".format(task_id, random.randint(1000, 10000), random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
