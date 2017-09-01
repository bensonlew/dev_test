# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
from biocluster.workflow import Workflow
import pymongo
from biocluster.config import Config
from biocluster.wpm.client import *
import datetime


class DemoInitWorkflow(Workflow):
    """
    初始化设置demo时进行demo备份
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DemoInitWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "task_id", "type": "string", "default": ""},  # 要设置为demo或取消的demo的task_id
            {"name": "type", "type": "string", "default": "ref_rna"},  # demo的类型
            {"name": "setup_type", "type": "string", "default": ""},  # 对demo进行的操作，设置为demo，取消删除demo
            {"name": "demo_number", "type": "int", "default": 1}  # demo备份的数量
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        self.start_listener()
        self.fire("start")
        if self.option("type") == "ref_rna":
            if self.option("setup_type") == "setup":
                from mbio.packages.rna.refrna_copy_demo import RefrnaCopyMongo
                target_project_sn = "refrna_demo"
                target_member_id = "refrna_demo"
                for i in range(self.option("demo_number")):
                    target_task_id = self.option("task_id") + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
                    copy_task = RefrnaCopyMongo(self.option("task_id"), target_task_id, target_project_sn, target_member_id)
                    copy_task.run()
            if self.option("setup_type") in ["cancel", "delete"]:
                from mbio.packages.rna.refrna_copy_delete import RefrnaCopyDelete
                RefrnaCopyDelete().find_task_id(task_id=self.option("task_id"))
        self.end()

    def end(self):
        super(DemoInitWorkflow, self).end()
