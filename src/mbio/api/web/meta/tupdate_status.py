# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log, config
import gevent
import urllib2
import random
import time
import hashlib
import urllib
from biocluster.config import Config
import json
from bson.objectid import ObjectId
from types import StringTypes
import datetime
import re
import os


class TupdateStatus(Log):

    def __init__(self, data):
        super(TupdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://www.tsanger.com/api/add_file"
        self._task_id = self.data.task_id
        self.db = self._config.get_db()
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB]
        self._sheetname = "update_info"

    def update(self):
        table_id = self.get_otu_id()
        while True:
            self._failed = False
            try:
                my_table_id = json.loads(table_id)
            except Exception:
                self.log("update_info:{}格式不正确".format(table_id))
                self._success = 0
                self._failed = True
                self._reject = 1
                break
            if self._try_times >= config.UPDATE_MAX_RETRY:
                self.log("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                self._reject = 1
                break
            try:
                if self._success == 0:
                    gevent.sleep(config.UPDATE_RETRY_INTERVAL)
                self._try_times += 1
                if my_table_id:
                    url_data = json.loads(self.data.data)
                    statu = url_data["content"]
                    # json_data = json.loads(statu, object_hook=date_hook)
                    json_data = statu
                    if "stage" in json_data.keys():
                        status = json_data["stage"]["status"]
                        desc = json_data["stage"]["error"]
                        create_time = json_data["stage"]["created_ts"]
                        self.update_log(my_table_id, status, desc, create_time)
                    else:
                        self._success = 0
                        self._failed = True
                        self._failed_times += 1
                        self._reject = 1
                        break
                else:
                    self._success = 0
                    self._failed = True
                    self._failed_times += 1
                    self._reject = 1
                    break
            except Exception, e:
                self._success = 0
                self._failed = True
                self._failed_times += 1
                self.log("又提交失败: %s" % e)
            else:
                self._success = 1
                self._failed = False
                self.log("提交成功")
                break
        self._end = True
        self.save()

    def get_otu_id(self):
        try:
            results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$id", vars={'id': self._task_id})
            if len(results) > 0:
                data = results[0]
                json_str = data.json
                json_obj = json.loads(json_str)
                return json_obj["options"][self._sheetname]
                # 返回mysql的workflow表的json这一列的option字段下的update_api字段下的值
            else:
                self.log("没有找到对应的任务:%s" % self._task_id)
        except Exception, e:
            self.log("任务ID查询异常: %s" % e)
        return False

    def update_log(self, id_value, status, desc, create_time):
        print self.post_data
        # print status
        for k in id_value:
            obj_id = k
            dbname = id_value[k]
            # print self.post_data
            collection = self.mongodb[dbname]
            if not isinstance(obj_id, ObjectId):
                if isinstance(obj_id, StringTypes):
                    obj_id = ObjectId(obj_id)
                else:
                    raise Exception("{}的值必须为ObjectId对象或其对应的字符串!".format(self._sheetname))
            try:
                print collection.find_one({"_id": obj_id})
            except:
                print "查询数据结果出错{}:{}".format(dbname, str(obj_id))
            create_time = str(create_time)
            if status == "finish":
                status = "end"
                desc = ""
            data = {
                "status": status,
                "desc": desc,
                "created_ts": create_time
            }
            collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)

            # 新建或更新sg_status表
            collection = self.mongodb['sg_status']
            if status == "start":
                tmp_col = self.mongodb[dbname]
                try:
                    tb_name = tmp_col.find_one({"_id": obj_id})["name"]
                except:
                    tb_name = ""
                tmp_task_id = list()
                print 'update_status task_id:', self._task_id
                tmp_task_id = re.split("_", self._task_id)
                tmp_task_id.pop()
                tmp_task_id.pop()
                print tmp_task_id
                insert_data = {
                    "table_id": obj_id,
                    "table_name": tb_name,
                    "task_id": "_".join(tmp_task_id),
                    "type_name": dbname,
                    "status": "start",
                    "is_new": "new",
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                print insert_data
                collection.insert_one(insert_data)
                print "insert into database success"
            elif status == "end":
                print "end"
                tmp_col = self.mongodb[dbname]
                my_params = tmp_col.find_one({"_id": obj_id})["params"]
                my_dict = json.loads(my_params)
                if "submit_location" in my_dict:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "params": my_params,
                        "submit_location": my_dict["submit_location"],
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                else:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "params": my_params,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                collection.find_one_and_update({"table_id": obj_id, "type_name": dbname}, {'$set': insert_data}, upsert=True)
                self.post_data_to_web()
            else:
                print "enter into else part"
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                tmp_col = self.mongodb[dbname]
                find_one = tmp_col.find_one({"_id": obj_id})
                if 'params' in find_one:
                    insert_data['params'] = find_one['params']
                if find_one['params']:
                    my_dict = json.loads(find_one['params'])
                    if "submit_location" in my_dict:
                        insert_data['submit_location'] = my_dict['submit_location']
                collection.find_one_and_update({"table_id": obj_id, "type_name": dbname}, {'$set': insert_data}, upsert=True)
            self._mongo_client.close()

    @property
    def post_data(self):
        """
        重写post_data
        从api_log里面读取data字段
        """
        data = json.loads(self.data.data)
        return data

    def _re_org_post(self, post_data):
        my_content = post_data["content"]
        my_stage = my_content["stage"]
        try:
            my_upload_files = post_data["upload_files"]
            target = my_upload_files[0]["target"]
            files = my_upload_files[0]["files"]
            new_files = list()
            new_dirs = list()
            for my_file in files:
                if my_file["type"] == "file":
                    tmp_dict = dict()
                    tmp_dict["path"] = os.path  .join(target, my_file["path"])
                    tmp_dict["size"] = my_file["size"]
                    tmp_dict["description"] = my_file["description"]
                    tmp_dict["format"] = my_file["format"]
                    new_files.append(tmp_dict)
                elif my_file["type"] == "dir":
                    tmp_dict = dict()
                    tmpPath = re.sub("\.$", "", my_file["path"])
                    tmp_dict["path"] = os.path.join(target, tmpPath)
                    tmp_dict["size"] = my_file["size"]
                    tmp_dict["description"] = my_file["description"]
                    tmp_dict["format"] = my_file["format"]
                    new_dirs.append(tmp_dict)
            # my_stage["fil es"] = new_files
            new_content = dict()
            new_content["files"] = new_files
            new_content["dirs"] = new_dirs
        except:
            new_content = dict()
        my_id = my_stage["task_id"]
        my_id = re.split('_', my_id)
        my_id.pop(-1)
        my_id.pop(-1)
        new_content["task_id"] = "_".join(my_id)
        my_data = dict()
        my_data["content"] = json.dumps(new_content)
        print my_data
        return urllib.urlencode(my_data)

    def post_data_to_web(self):
        my_post_data = self._re_org_post(self.post_data)
        self._post_data = "%s&%s" % (self.get_sig(), my_post_data)
        try:
            response = self.send()
            code = response.getcode()
            response_text = response.read()
            print("Return page:\n%s" % response_text)
        except urllib2.HTTPError as e:
            self._success = 0
            self._response_code = e.code
            self._reject = 1
            raise Exception("提交失败：%s" % e)
        except Exception as e:
            self._success = 0
            self._reject = 1
            raise Exception("提交失败: %s" % e)
        else:
            try:
                response_json = json.loads(response_text)
            except Exception as e:
                self._response_code = code
                self._response = response_text
                self._success = 0
                self._reject = 1
                raise Exception("提交失败: 返回数据类型不正确 %s" % e)
            else:
                self._response_code = code
                self._response = response_text
                if response_json["success"] == "true" \
                        or response_json["success"] is True or response_json["success"] == 1:
                    self._success = 1
                else:
                    self._success = 0
                    self._reject = 1
                    self._failed = True
                    raise Exception("提交被拒绝，终止提交:%s" % response_json["message"])

    def get_sig(self):
        nonce = str(random.randint(1000, 10000))
        timestamp = str(int(time.time()))
        x_list = [self._key, timestamp, nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        sig = sha1.hexdigest()
        signature = {
            "client": self._client,
            "nonce": nonce,
            "timestamp": timestamp,
            "signature": sig
        }
        return urllib.urlencode(signature)


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return json_dict
