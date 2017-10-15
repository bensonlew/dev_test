# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
from bson import SON
from biocluster.config import Config
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
import random
import json

class Metagenomic(Meta):
    def __init__(self):
        self.db_name = Config().MONGODB + '_metagenomic'
        super(RefRna, self).__init__(db=self.db_name)

    def get_new_id(self, task_id, main_id):
        new_id = "%s_%s_%s" % (task_id, main_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, main_id)
        return new_id

    def get_main_info(self, main_id, collection_name):
        if isinstance(main_id, types.StringTypes):
            main_id = ObjectId(main_id)
        elif isinstance(main_id, ObjectId):
            main_id = main_id
        else:
            raise Exception("输入main_id参数必须为字符串或者ObjectId类型!")
        collection = self.db[collection_name]
        main_info = collection.find_one({'_id': main_id})
        return main_info

    def insert_main_table(self, collection_name, data):
        return self.db[collection_name].insert_one(SON(data)).inserted_id
    
    

    def insert_geneset_info(self, geneset_id, col_name, col_id):
        mongodb = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        collection = mongodb['sg_geneset']
        geneset_list = []
        if not isinstance(geneset_id, types.StringTypes):
            # geneset_id = ObjectId(geneset_id)
            raise Exception("输入geneset_id参数必须为字符串类型!")
        geneset_list.extend([ObjectId(x) for x in geneset_id.split(",")])
        if isinstance(col_id, types.StringTypes):
            col_id = ObjectId(col_id)
        elif isinstance(col_id, ObjectId):
            pass
        else:
            raise Exception("输入col_id参数必须为字符串或者ObjectId类型!")
        try:
            for geneset_id in geneset_list:
                result = collection.find_one({"_id":geneset_id})
                result["is_use"] = 1
                collection.find_one_and_update({"_id":geneset_id},{"$set":result})
        except Exception:
            print "没有找到geneset_id:{}".format(geneset_id)
        try:
            collection = mongodb["col_name"]
            collection.find_one({"_id":col_id})
        except:
            "没有找到col_id:{} in {}".format(col_id, col_name)
        for geneset_id in geneset_list:
            opts = {
                "geneset_id": geneset_id,
                "col_name": col_name,
                "col_id": col_id
            }
            collection = mongodb["sg_geneset_info"]
            collection.insert_one(opts)
        return True

    def delete_geneset(self, geneset_id):
        if isinstance(geneset_id, types.StringTypes):
            geneset_id = ObjectId(geneset_id)
        elif isinstance(data.geneset_id, ObjectId):
            pass
        else:
            raise Exception("输入geneset_id参数必须为字符串或者ObjectId类型!")
        mongodb = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        collection = mongodb['sg_geneset_info']
        results = collection.find({"geneset_id":geneset_id})
        for result in results:
            col_name = result["col_name"]
            col_id = result["col_id"]
            print col_id
            col = mongodb[col_name]
            print col_name
            try:
                col_result = col.find_one({"_id":col_id})
                col_result["params"] = ""
                col.find_one_and_update({"_id":col_id}, {"$set":col_result})
            except:
                print "不能找到对应id{} in {}".format(col_id, col_name)
        collection = mongodb["sg_geneset"]
        result = collection.find_one({"_id":geneset_id})
        if result:
            collection.remove({"_id":geneset_id})
        return True

   

    def insert_seq(self, mongo_data):
        task_id = mongo_data["task_id"]
        mongodb = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        collection = mongodb['sg_query_seq']
        result = collection.find_one({"task_id": task_id})
        if result:
            collection.find_one_and_update({"task_id": task_id}, {"$set": mongo_data})
        else:
            collection.insert_one(mongo_data)

if __name__ == "__main__":
    data=RefRna()
    d = data.get_express_id("tsg_2000","fpkm","featurecounts")
    #print d
    data.get_control_id("5924f2a77f8b9a201d8b4567")
