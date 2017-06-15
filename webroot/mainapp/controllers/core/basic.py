# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import traceback
import time
import json
from mainapp.models.mongo.meta import Meta
from biocluster.wpm.client import worker_client, wait
from biocluster.config import Config
from bson.objectid import ObjectId


class Basic(object):
    def __init__(self, data, instant=False):
        self._instant = instant
        self._json = data
        self._id = data["id"]
        self._return_msg = None
        self._mongo_client = Config().mongo_client
        if "db_type" in data.keys() and data['db_type'] == '_nipt':
            self.mongodb = self._mongo_client[Config().MONGODB + '_nipt']
        elif "db_type" in data.keys() and data['db_type'] == '_ref_rna':
            self.mongodb = self._mongo_client[Config().MONGODB + '_ref_rna']
        else:
            self.mongodb = self._mongo_client[Config().MONGODB]


    @property
    def id(self):
        """
        获取运行任务的ID

        :return:
        """
        return self._id

    @property
    def instant(self):
        """
        任务是否是即时计算

        :return: bool
        """
        return self._instant

    @property
    def return_msg(self):
        """
        获取运行任务的返回值

        :return:
        """
        return self._return_msg

    def run(self):
        if "main_table_data" in self._json['options'].keys():
            main_table_data = self._json['options']['main_table_data']
        else:
            main_table_data = None
        if 'update_info' in self._json['options'].keys():
            update_info = json.loads(self._json["options"]['update_info'])
        else:
            update_info = None
        if "main_table_data" in self._json['options'].keys():
            del self._json['options']['main_table_data']

        # worker = worker_client()
        # info = worker.add_task(self._json)
        # if "success" in info.keys() and info["success"]:
        #     if update_info and main_table_data:
        #         for i in update_info:
        #             if i == "batch_id":
        #                 continue
        #             Meta().insert_main_table_new(update_info[i], i, main_table_data)
        #         if self.instant:
        #             return self.instant_wait(worker)
        #         else:
        #             return info
        # else:
        #     raise Exception("任务提交失败:%s" % info["info"])
        try:
            worker = worker_client()
            info = worker.add_task(self._json)
            if "success" in info.keys() and info["success"]:
                if update_info and main_table_data:
                    for i in update_info:
                        if i == "batch_id":
                            continue
                        self.insert_main_table_new(update_info[i], i, main_table_data)
                if self.instant:
                    return self.instant_wait(worker)
                else:
                    return info
            else:
                return {"success": False, "info": "任务提交失败 %s" % (info["info"])}
                # raise Exception("任务提交失败:%s" % info["info"])
        except Exception, e:
            exstr = traceback.format_exc()
            print "ERROR:", exstr
            raise Exception("任务提交失败：%s, %s" % (str(e), str(exstr)))

    def instant_wait(self, worker):
        end = wait(self._id)
        if end is True:
            self._return_msg = worker.get_msg(self._id)
            return self._return_msg
        else:
            raise Exception("运行超时!")

    def insert_main_table_new(self, collection, obj_id, data):
        return self.mongodb[collection].find_one_and_update({"_id": ObjectId(obj_id)}, {'$set': data}, upsert=True)
