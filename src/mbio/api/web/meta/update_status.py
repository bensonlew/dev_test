# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
# last_modified = shenghe
import urllib
import json
import datetime
import re
import sys
from bson.objectid import ObjectId
from biocluster.wpm.log import Log
from biocluster.core.function import CJsonEncoder, filter_error_info
import traceback


class UpdateStatus(Log):
    """
    meta的web api，用于更新sg_status表并向前端发送状态信息和文件上传信息
    一般可web api功能可从此处继承使用，需要重写__init__方法
    """

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://api.sanger.com/task/add_file"

    def __del__(self):
        self._mongo_client.close()

    @property
    def post_data(self):
        workflow_id = self.data["sync_task_log"]['task']["task_id"]
        my_id = re.split('_', workflow_id)
        my_id.pop(-1)
        my_id.pop(-1)
        data = dict()
        content = {
            "task": {
                "task_id": "_".join(my_id)
            }
        }
        if 'files' in self.data['sync_task_log'].keys():
            content['files'] = self.data["sync_task_log"]["files"]
        if 'dirs' in self.data['sync_task_log'].keys():
            content['dirs'] = self.data['sync_task_log']['dirs']
        if 'base_path' in self.data['sync_task_log'].keys():
            content['base_path'] = self.data['sync_task_log']['base_path']
        data['sync_task_log'] = json.dumps(content, cls=CJsonEncoder)
        return urllib.urlencode(data)

    def update(self):
        if 'files' in self.data['sync_task_log'].keys() or 'dirs' in self.data['sync_task_log'].keys():
            self.send()

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
        batch_id = self.update_info.pop("batch_id") if 'batch_id' in self.update_info else None
        for obj_id, collection_name in self.update_info.items():
            obj_id = ObjectId(obj_id)
            collection = self.db[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.db['sg_status']
            if status == "start":
                if not batch_id:
                    tmp_col = self.db[collection_name]
                    try:
                        temp_find = tmp_col.find_one({"_id": obj_id})
                        tb_name = temp_find["name"]
                        temp_params = temp_find['params']
                        submit_location = json.loads(temp_params)['submit_location']
                    except Exception, e:
                        exstr = traceback.format_exc()
                        print exstr
                        print e
                        sys.stdout.flush()
                        tb_name = ""
                        temp_params = ''
                        submit_location = ''
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

    def pipe_update(self, batch_id, collection, _id, status, desc):
        if not batch_id:
            return
        batch_id = ObjectId(batch_id)
        if str(collection) == "sg_otu" and str(status) == "failed" \
                and self.db['sg_otu'].find_one({"_id": ObjectId(_id)})['type'] == "otu_statistic":
            #  用于一键化抽平分析出错后面依赖不能进行报错
            self.db['sg_pipe_detail'].insert_one({'pipe_batch_id': batch_id, "table_id": ObjectId(_id),
                                                       'group_name': "", 'level_id': "",
                                                       "submit_location": "otu_statistic", 'status': "failed",
                                                       'desc': "因为OtuSubsample分析计算失败，后面的依赖分"
                                                               "析都不能进行，请重新设定基本参数，再次尝试!"})
            self.db['sg_pipe_batch'].find_one_and_update({"_id": batch_id},
                                                              {'$set': {"ends_count": 1, "all_count": 1}}, upsert=True)
            pass
        # elif str(collection) == "sg_alpha_diversity" and str(status) == "failed":
        #     pass
        else:

            # self.mongodb['sg_pipe_batch'].find_one_and_update({'_id': batch_id}, {"$inc": {"ends_count": 1}})
            self.logger.info("pipe_batch_id: {}, status: {}, desc:{}".format(batch_id, status, desc))
            self.db['sg_pipe_detail'].find_one_and_update({'pipe_batch_id': batch_id, "table_id": ObjectId(_id)},
                                                               {"$set": {'status': status, "desc": desc}})
            end_counts = self.db['sg_pipe_detail'].find({'pipe_batch_id': batch_id,
                                                              'status': {'$in': ['end', "failed"]}}).count()
            self.logger.info("查询end_counts个数:{}".format(end_counts))  # 测试完成后删除
            # 多样性指数失败，则T检验失败，但是t检验失败后没有插表，这里通过判断多样性指数失败个数来间接判断t检验
            diversity_end_counts = self.db['sg_pipe_detail'].find({'pipe_batch_id': batch_id,
                                                                        "submit_location":
                                                                            {'$in': ["alpha_diversity_index",
                                                                                     "otu_pan_core"]},
                                                                        'status': "failed"}).count()
            self.logger.info("查询diversity_end_counts个数:{}".format(diversity_end_counts))  # 测试完成后删除
            update_counts = end_counts + diversity_end_counts
            self.logger.info("查询update_counts个数:{}".format(update_counts))
            self.db['sg_pipe_batch'].find_one_and_update({"_id": batch_id},
                                                              {'$set': {"ends_count": update_counts}}, upsert=True)
