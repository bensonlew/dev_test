# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
# last_modified = shenghe
import urllib
import json
import datetime
import re
import gevent
import urllib2
import sys
from bson.objectid import ObjectId
from biocluster.wpm.log import Log
from biocluster.config import Config
from biocluster.core.function import CJsonEncoder, filter_error_info


class UpdateStatus(Log):
    """
    meta的web api，用于更新sg_status表并向前端发送状态信息和文件上传信息
    一般可web api功能可从此处继承使用，需要重写__init__方法
    """

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://api.sanger.com/task/add_file"
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB]

    def get_post_data(self):
        # ana_dir = {}
        # report_dir_des = {}
        # n = 0
        workflow_id = self.data["sync_task_log"]['task']["task_id"]
        my_id = re.split('_', workflow_id)
        my_id.pop(-1)
        my_id.pop(-1)
        data = dict()
        # assert len(my_id) == 2
        content = {
            "task": {
                "task_id": "_".join(my_id)
            }
            # "stage": self.data["content"]["stage"]
        }
        if 'files' in self.data['sync_task_log']:
            content['files'] = self.data["sync_task_log"]["files"]
        if 'dirs' in self.data['sync_task_log']:
            content['dirs'] = self.data['sync_task_log']['dirs']
            # min_path_len = len(content['dirs'][0]['path'].rstrip('/').split("/"))
            # for m in content['dirs'][1:]:
            #     if len(m['path'].rstrip('/').split("/")) < min_path_len:
            #         min_path_len = len(m['path'].rstrip('/').split("/"))
            # for m in content['dirs']:
            #     if len(m['path'].rstrip('/').split("/")) == min_path_len:
            #         n += 1
            #         output_dir = os.path.dirname(m['path'].rstrip('/'))
            #         ana_dir = {'path': output_dir, "size": "", "description": m['description'], "format": ""}
            #         report_dir = os.path.dirname(output_dir.rstrip("/"))
            #         report_dir_des = {'path': report_dir, "size": "", "description": "交互分析结果文件夹", "format": ""}
            # content['dirs'].append(ana_dir)
            # content['dirs'].append(report_dir_des)
            # if n > 1:
            #     raise Exception("存在两个最短路径，请进行检查！")
        data['sync_task_log'] = json.dumps(content, cls=CJsonEncoder)
        self.logger.info("CONTENT:{}".format(data['sync_task_log']))
        return urllib.urlencode(data)

    def update(self):

        self.update_status()
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
                # self.update_status()
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
        # self.save()

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
        self.update_info = json.loads(self.update_info)
        # meta_pipe_detail_id = self.update_info.pop("meta_pipe_detail_id") if 'meta_pipe_detail_id'  \
        #                                                                      in self.update_info else None
        batch_id = self.update_info.pop("batch_id") if 'batch_id' in self.update_info else None
        for obj_id, collection_name in self.update_info.items():
            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.mongodb['sg_status']
            if status == "start":
                if not batch_id:
                    tmp_col = self.mongodb[collection_name]
                    try:
                        temp_find = tmp_col.find_one({"_id": obj_id})
                        tb_name = temp_find["name"]
                        temp_params = temp_find['params']
                        submit_location = json.loads(temp_params)['submit_location']
                    except:
                        tb_name = ""
                        temp_params = ''
                        submit_location = ''
                    # tmp_task_id = list()
                    print 'update_status task_id:', self.task_id
                    tmp_task_id = re.split("_", self.task_id)
                    tmp_task_id.pop()
                    tmp_task_id.pop()
                    insert_data = {
                        "table_id": obj_id,
                        "table_name": tb_name,
                        "task_id": "_".join(tmp_task_id),
                        "type_name": collection_name,
                        "params": temp_params,
                        "submit_location": submit_location,
                        "status": "start",
                        "is_new": "new",
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    sg_status_col.insert_one(insert_data)
            elif status == "finish":  # 只能有一次finish状态
                if not batch_id:
                    insert_data = {
                        "status": 'end',
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                self.pipe_update(batch_id, collection_name, obj_id, "end", desc)
            else:
                if not batch_id:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                self.pipe_update(batch_id, collection_name, obj_id, status, desc)
            self._mongo_client.close()

    def pipe_update(self, batch_id, collection, _id, status, desc):
        if not batch_id:
            # self.logger.info("没有batchID，无法更新相关表格")
            return
        self.logger.info("存在batch_id:{}, collection: {}, _id: {}, status: {}"
                         .format(batch_id, collection, _id, status))
        # meta_pipe_detail_id = ObjectId(meta_pipe_detail_id)
        batch_id = ObjectId(batch_id)
        if str(collection) == "sg_otu" and str(status) == "failed" \
                and self.mongodb['sg_otu'].find_one({"_id": ObjectId(_id)})['type'] == "otu_statistic":
            self.logger.info("抽平不加")
            pass
        elif str(collection) == "sg_alpha_diversity" and str(status) == "failed":
            self.logger.info("多样性指数不加")
            pass
        else:
            self.mongodb['sg_pipe_batch'].find_one_and_update({'_id': batch_id}, {"$inc": {"ends_count": 1}})
            self.mongodb['sg_pipe_detail'].find_one_and_update({'pipe_batch_id': batch_id, "table_id": ObjectId(_id)},
                                                               {"$set": {'status': status, "desc": desc}})
