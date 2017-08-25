# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import web
import json
import traceback
from mainapp.libs.signature import check_sig
from mainapp.controllers.core.basic import Basic
from biocluster.core.function import filter_error_info
from mainapp.models.workflow import Workflow
# from mainapp.models.mongo.ref_rna import RefRna
from biocluster.wpm.client import worker_client, wait
import random


class DemoInit(object):
    @check_sig
    def POST(self):
        data = web.input()
        requires = ['type', 'task_id', 'setup_type']
        for i in requires:
            if not (hasattr(data, i)):
                return json.dumps({"success": False, "info": "缺少%s参数!" % i})
        workflow_id = self.get_new_id(data.task_id)
        if data.type == "ref_rna":
            data = {
              'id': workflow_id,
              'stat_id': 0,
              'name': , # 需要配置
              'client': data.client,
              'options': {
                  "task_id": data.task_id,
                  "setup_type": data.setup_type
              }
            }
        # workflow_client = Basic(data=data, instant=False)
        try:
            worker = workflow_client()
            info = worker.add_task(data)
            # if "success" in info.keys() and info["success"]:
            #     pass
            # else:
            #     return {"success": False, "info": "任务提交失败%s" % (info["info"])}
        except:
            return {"success": False, "info": "任务提交失败%s" % (info["info"])}
        # except Exception, e:
        #     exstr = traceback.format_exc()
        #     print "ERROR:", exstr
        #     raise Exception("任务提交失败：%s, %s" % (str(e), str(exstr)))

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
