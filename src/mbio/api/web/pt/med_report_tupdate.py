# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import json
import datetime
import urllib2

import gevent
import sys
from bson.objectid import ObjectId
from biocluster.config import Config
from biocluster.core.function import filter_error_info
from mbio.api.web.meta.update_status import UpdateStatus
from biocluster.wpm.log import Log


class MedReportTupdate(Log):

    def __init__(self, data):
        super(MedReportTupdate, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        # self._url = "http://api.tsanger.com/task/add_file"
        self._url = "http://api.tsg.com/task/add_file"
        self._mongo_client = self._config.mongo_client
        self.database = self._mongo_client[Config().MONGODB+'_paternity_test']

    def update(self):

        while True:
            if self._try_times >= self.config.UPDATE_MAX_RETRY:
                self.logger.info("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                self._reject = 1
                break
            try:
                if self._success == 0:
                    gevent.sleep(self.config.UPDATE_RETRY_INTERVAL)
                self._try_times += 1
                response = self.send()
                code = response.getcode()
                response_text = response.read()
                self.update_status()
                print "Return page:\n%s" % response_text
                sys.stdout.flush()
            except urllib2.HTTPError, e:
                self._success = 0
                self._failed_times += 1
                self._response_code = e.code
                self.logger.warning("提交失败：%s, 重试..." % e)
            except Exception, e:
                self._success = 0
                self._failed_times += 1
                self.logger.warning("提交失败: %s, 重试..." % e)
            else:
                try:
                    response_json = json.loads(response_text)
                except Exception, e:
                    self._response_code = code
                    self._response = response_text
                    self._success = 0
                    self._failed_times += 1
                    self.logger.error("提交失败: 返回数据类型不正确 %s ，重试..." %  e)
                else:
                    self._response_code = code
                    self._response = response_text
                    if response_json["success"] == "true" \
                            or response_json["success"] is True or response_json["success"] == 1:
                        self._success = 1
                        self.logger.info("提交成功")
                    else:
                        self._success = 0
                        self._failed_times += 1
                        self._reject = 1
                        self._failed = True
                        self.logger.error("提交被拒绝，终止提交:%s" % response_json["message"])
                    break
        self._end = True
        self.model.save()

    def update_status(self):
        status = self.data["sync_task_log"]["task"]["status"]
        desc = ''
        for i in self.data['sync_task_log']['log']:
            if 'name' not in i:
                desc = i['desc']
        desc = filter_error_info(desc)
        create_time = str(self.data["sync_task_log"]["task"]["created_ts"])

        if not self.update_info:
            return
        for obj_id, collection_name in json.loads(self.update_info).items():
            obj_id = ObjectId(obj_id)
            collection = self.database[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "time": create_time
                }
                collection.update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.database[collection_name]
            if status == "start":
                tmp_col = self.database[collection_name]
                try:
                    temp_find = tmp_col.find_one({"_id": obj_id})
                    tb_name = temp_find["name"]
                except:
                    tb_name = ""
                    temp_params = ''
                    submit_location = ''
                insert_data = {
                    "status": "start",
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id}, {'$set': insert_data}, upsert=True)
            elif status == "finish":  # 只能有一次finish状态
                insert_data = {
                    "status": 'end',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id},
                                                  {'$set': insert_data}, upsert=True)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id},
                                                  {'$set': insert_data}, upsert=True)
            self._mongo_client.close()
