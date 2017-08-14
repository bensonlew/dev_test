# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

""""""

from biocluster.workflow import Workflow
import pymongo
from biocluster.config import Config


class RefrnaCopyDemoWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RefrnaCopyDemoWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "task_id", "type": 'string', "default": ''},
            {"name": "target_task_id", "type": 'string', "default": ''},
            {"name": "target_project_sn", "type": 'string', "default": ''},
            {"name": "target_member_id", "type": 'string', "default": ''}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def check(self):
        pass

    def run(self):
        self.start_listener()
        self.fire("start")
        # from mbio.packages.rna.refrna_copy_demo import RefrnaCopyMongo
        # copy_task = RefrnaCopyMongo(self.option("task_id"), self.option("target_task_id"), self.option("target_project_sn"), self.option("target_member_id"))
        # copy_task.run()
        old_task_id = self.old_task_id()
        self.logger.info(old_task_id)
        self.update_task_id(old_task_id, self.option("target_task_id"))
        self.end()

    def old_task_id(self):
        # client = pymongo.MongoClient('mongodb://192.168.10.188:27017/')
        # db = client['sanger_ref_rna']
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        col = db["sg_task"]
        results = col.find()
        for result in results:
            old_task_id = result["task_id"]
            if old_task_id.startswith("refrna_demo_mouse"):
                col.find_one_and_update({"_id": result["_id"]}, {"$set":{"task_id":self.option("target_task_id")}})
                col.find_one_and_update({"_id": result["_id"]}, {"$set": {"member_id": self.option("target_member_id")}})
                col.find_one_and_update({"_id": result["_id"]}, {"$set": {"project_sn": self.option("target_project_sn")}})
                return old_task_id

    def update_task_id(self, old_task_id, new_task_id):
        # client = pymongo.MongoClient('mongodb://192.168.10.188:27017/')
        # db = client['sanger_ref_rna']
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        col_list = []
        for col_name in db.collection_names():
            col = db[col_name]
            result = col.find_one()
            try:
                if "task_id" in result:
                    col_list.append(col_name)
            except:
                continue
        for col_name in col_list:
            col = db[col_name]
            results = col.find({"task_id": old_task_id})
            for result in results:
                col.update_one({"_id": result["_id"]}, {"$set": {"task_id": new_task_id}})
            # if col_name == "sg_task":
            #     col.update_one({"_id": result["_id"]}, {"$set": {"member_id": self.option("target_member_id")}})
            #     col.update_one({"_id": result["_id"]}, {"$set": {"project_sn": self.option("target_project_sn")}})


    def end(self):
        super(RefrnaCopyDemoWorkflow, self).end()
