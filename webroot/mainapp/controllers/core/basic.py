# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import traceback
import time
import json
from mainapp.models.mongo.meta import Meta
from biocluster.wpm.client import worker_client, wait


class Basic(object):
    def __init__(self, data, instant=False):
        self._instant = instant
        self._json = data
        self._id = data["id"]
        self._return_msg = None

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
        try:
            worker = worker_client()
            info = worker.add_task(self._json)
            if "success" in info.keys() and info["success"]:
                if update_info and main_table_data:
                    for i in update_info:
                        if i == "batch_id":
                            continue
                        Meta().insert_main_table_new(update_info[i], i, main_table_data)
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

            # raise Exception("任务提交失败：%s" % (str(e)))
            # try:
            #     worker = worker_client()
            #     info = worker.add_task(self._json)
            #     if "success" in info.keys() and info["success"]:
            #         for obj_id, collection_name in update_info.items():
            #             Meta().insert_main_table_new(collection_name, obj_id, main_table_data)
            #         if self.instant:
            #             return self.instant_wait(worker)
            #         else:
            #             return info
            #     else:
            #         return {"success": False, "info": "%s"%(info["info"])}
            # except Exception as e:
                # try:
                #     worker = worker_client()
                #     info = worker.add_task(self._json)
                #     if "success" in info.keys() and info["success"]:
                #         if self.instant:
                #             return self.instant_wait(worker)
                #         else:
                #             return info
                #     else:
                #         return {"success": False, "info": "%s"%(info["info"])}
            #     except Exception, e:
            #     #     exstr = traceback.format_exc()
            #         raise Exception("任务提交失败：%s" % (str(e)))

    def instant_wait(self, worker):
        end = wait(self._id)
        if end is True:
            self._return_msg = worker.get_msg(self._id)
            return self._return_msg
        else:
            raise Exception("运行超时!")
