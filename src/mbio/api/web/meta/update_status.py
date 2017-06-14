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
        if 'files' in self.data['sync_task_log'].keys():
            content['files'] = self.data["sync_task_log"]["files"]
        if 'dirs' in self.data['sync_task_log'].keys():
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
        self.logger.info("开始执行update_status")
        self.update_status()
        self.logger.info("执行update_status")
        if 'files' in self.data['sync_task_log'].keys() or 'dirs' in self.data['sync_task_log'].keys():
            self.logger.info("执行post")
            while True:
                self.logger.info("执行post111")
                if self._try_times >= self.config.UPDATE_MAX_RETRY:
                    self.logger.info("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                    self._failed = True
                    self._reject = 1
                    break
                try:
                    self.logger.info("test01")
                    if self._success == 0:
                        gevent.sleep(self.config.UPDATE_RETRY_INTERVAL)
                    self._try_times += 1
                    response = self.send()
                    self.logger.info("test02")
                    code = response.getcode()
                    self.logger.info("test03")
                    response_text = response.read()
                    self.logger.info("test04")
                    # self.update_status()
                    print "Return page:\n%s" % response_text
                    sys.stdout.flush()
                    self.logger.info("test05")
                except urllib2.HTTPError, e:
                    self.logger.info("test06")
                    self._success = 0
                    self._failed_times += 1
                    self._response_code = e.code
                    self.logger.warning("提交失败：%s, 重试..." % e)
                except Exception, e:
                    self.logger.info("test07")
                    self._success = 0
                    self._failed_times += 1
                    self.logger.warning("提交失败: %s, 重试..." % e)
                else:
                    try:
                        self.logger.info("test08")
                        response_json = json.loads(response_text)
                        self.logger.info("test09")
                    except Exception, e:
                        self.logger.info("test10")
                        self._response_code = code
                        self._response = response_text
                        self._success = 0
                        self._failed_times += 1
                        self.logger.error("提交失败: 返回数据类型不正确 %s ，重试..." %  e)
                    else:
                        self.logger.info("test11")
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
        self.logger.info("执行1")
        status = self.data["sync_task_log"]["task"]["status"]
        desc = ''
        self.logger.info("输出状态status%s"%(status))
        for i in self.data['sync_task_log']['log']:
            if 'name' not in i:
                desc = i['desc']
        desc = filter_error_info(desc)
        self.logger.info("输出desc%s" % (desc))
        create_time = str(self.data["sync_task_log"]["task"]["created_ts"])
        self.logger.info("输出create_time%s" % (create_time))
        if not self.update_info:
            return
        self.update_info = json.loads(self.update_info)
        self.logger.info("输出self.update_info%s" % (self.update_info))
        # meta_pipe_detail_id = self.update_info.pop("meta_pipe_detail_id") if 'meta_pipe_detail_id'  \
        #                                                                      in self.update_info else None
        batch_id = self.update_info.pop("batch_id") if 'batch_id' in self.update_info else None
        self.logger.info("输出batch_id%s" % (batch_id))
        for obj_id, collection_name in self.update_info.items():

            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            self.logger.info("输出obj_id：%s， collection：%s" % (batch_id, collection))
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                self.logger.info("开始更新主表")
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
                self.logger.info("更新完一次主表")
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
                    # print 'update_status task_id:', self.task_id
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
                    self.logger.info("插入状态表1")
                    sg_status_col.insert_one(insert_data)
                    self.logger.info("插入状态表1ok")
            elif status == "finish":  # 只能有一次finish状态
                if not batch_id:
                    insert_data = {
                        "status": 'end',
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.logger.info("插入状态表2")
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                    self.logger.info("插入状态表2ok")
                self.logger.info("插入一键化状态表1")
                self.pipe_update(batch_id, collection_name, obj_id, "end", desc)
                self.logger.info("插入一键化状态表1ok")
            else:
                if not batch_id:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.logger.info("插入状态表3")
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                    self.logger.info("插入状态表3ok")
                self.logger.info("插入一键化状态表2")
                self.pipe_update(batch_id, collection_name, obj_id, status, desc)
                self.logger.info("插入一键化状态表2ok")
            # self._mongo_client.close()

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
            #  用于一键化抽平分析出错后面依赖不能进行报错
            self.mongodb['sg_pipe_detail'].insert_one({'pipe_batch_id': batch_id, "table_id": ObjectId(_id),
                                                       'group_name': "", 'level_id': "",
                                                       "submit_location": "otu_statistic", 'status': "failed",
                                                       'desc': "因为OtuSubsample分析计算失败，后面的依赖分析都不能进行，请重新设定基本参数，再次尝试!"})
            pass
        elif str(collection) == "sg_alpha_diversity" and str(status) == "failed":
            self.logger.info("多样性指数不加")
            pass
        else:
            self.logger.info("更新一键化状态表3")
            self.mongodb['sg_pipe_batch'].find_one_and_update({'_id': batch_id}, {"$inc": {"ends_count": 1}})
            self.mongodb['sg_pipe_detail'].find_one_and_update({'pipe_batch_id': batch_id, "table_id": ObjectId(_id)},
                                                               {"$set": {'status': status, "desc": desc}})
            self.logger.info("更新一键化状态表3ok")
