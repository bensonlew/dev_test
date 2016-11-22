#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import threading
import sys
import pickle
from biocluster.core.function import load_class_by_path
# import json
from biocluster.wsheet import Sheet
# import importlib
# from biocluster.config import Config
from bson import ObjectId, SON
import datetime
import os
import json
import traceback


def task_obj_exit(obj, exitcode=1, data="", terminated=False):
    """
    立即退出当前流程

    :param exitcode:
    :return:
    """
    update_data = {
        "is_error": 1,
        "error": "程序主动退出:%s" % data,
        "end_time": datetime.datetime.now(),
        "is_end": 1,
        "workdir": obj.work_dir,
        "output": obj.output_dir
    }
    obj._update(update_data)
    if terminated:
        obj.step.terminated(data)
    else:
        obj.step.failed(data)
    obj.step.update()
    obj.end_unfinish_job()
    obj.logger.info("程序退出: %s " % data)
    obj.rpc_server.close()
    obj.exit_data = data
    raise Exception(data)


class Instant(object):
    """
    """
    def __init__(self):
        self._sheet = self.get_sheet()
        self._workflow = load_class_by_path(self.sheet.name, tp='Workflow')
        self._workflow.exit = task_obj_exit
        self._task_object = self._workflow(self.sheet)
        self.logger = self._task_object.logger
        self._mongo_ids = None
        self.return_info = {"success": False, "info": "程序非正常结束"}
        self._uploadDirObj = list()
        self._work_dir = os.path.dirname(self._sheet_pk)
        self.config = self._task_object.config
        self._db = self.config.mongo_client[self.config.MONGODB]

    def run(self):
        try:
            # self._task_object.exit = task_obj_exit
            self._task_object.run()
            if hasattr(self._task_object, "exit_data"):
                raise Exception(self._task_object.exit_data)
            self._mongo_ids = self._task_object.return_mongo_ids
            for i in self._mongo_ids:
                if i["add_in_sg_status"]:
                    self.add_sg_status(i['id'], i['collection_name'], i['desc'])
            self._uploadDirObj = self._task_object._upload_dir_obj
            self.format_result()
            # 整理返回前端结果
        except Exception as e:
            traceback_err = traceback.format_exc()
            print traceback_err
            self.logger.error(traceback_err)
            self.return_info = {"success": False, "info": "程序运行过程中发生错误，错误信息:{}".format(e)}
        self.pickle_result()

    def pickle_result(self):
        self.pickle_result_file = self._work_dir + '/return_web.pk'
        with open(self.pickle_result_file, 'wb') as w:
            pickle.dump(self.return_info, w)

    @property
    def sheet(self):
        return self._sheet

    @sheet.setter
    def sheet(self, value):
        if not isinstance(value, Sheet):
            raise Exception("sheet值必须为Sheet对象!")
        self._sheet = value

    @property
    def db(self):
        return self._db

    def add_sg_status(self, table_id, type_name, desc=None):
        collection = self.db["sg_status"]
        task_id = '_'.join(self.sheet.id.split('_')[0:-2])
        table_id = ObjectId(table_id)
        temp_collection = self.db[type_name]
        tempfind = temp_collection.find_one({'_id': table_id})
        if not tempfind:
            raise Exception('提供的ID无法在表:%s中找到' % type_name)
        table_name = tempfind['name']
        params = tempfind['params']
        dict_params = json.loads(params)
        insert_data = {
            "table_id": table_id,
            "table_name": table_name,
            "task_id": task_id,
            "type_name": type_name,
            "status": 'end',
            "is_new": 'new',
            "desc": desc,
            'params': params,
            "submit_location": dict_params['submit_location'],
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        insert_data = SON(insert_data)
        return_id = collection.insert_one(insert_data).inserted_id
        if return_id:
            self.logger.info('Insert sg_status ID:{}'.format(return_id))
            return str(return_id)
        else:
            self.logger.error('插入sg_status表出错')
            raise Exception('插入sg_status表出错')

    @property
    def mongo_ids(self):
        return self._mongo_ids

    def format_result(self):
        content = dict()
        content["dirs"] = list()
        content["files"] = list()
        content["ids"] = list()
        for one_insert in self.mongo_ids:
            idDict = {'id': str(one_insert['id']),
                      'name': self.get_main_table_name(one_insert['collection_name'], str(one_insert['id']))}
            content["ids"].append(idDict)
        if len(self.mongo_ids) == 1:
            content['ids'] = content['ids'][0]
        files, dirs = self.get_upload_files()
        content['files'] = files
        content['dirs'] = dirs
        info = dict()
        info["success"] = True
        info["content"] = content
        self.return_info = info

    def get_main_table_name(self, table, main_id):
        """
        查询数据库获取主表名称name
        """
        table_name = self.db[table].find_one({'_id': ObjectId(main_id)})['name']
        if not table_name:
            raise Exception('在表:{} 中未找到_id为:{} 的数据，或者表中没有"name"字段'.format(table, main_id))
        return table_name

    def get_upload_files(self):
        """将workflow的文件上传对象的文件列表取出，用于返回前端"""
        return_files = []
        return_dirs = []

        def create_path(path, dir_path):
            dir_path = os.path.split(dir_path)[1]
            return self.sheet.output + '/' + dir_path + '/' + path.lstrip('.')
        for i in self._uploadDirObj:
            for one in i.file_list:
                if one['type'] == 'file':
                    return_files.append({
                        "path": create_path(one["path"], i.path),
                        "format": one["format"],
                        "description": one["description"],
                        "size": one["size"]
                    })
                elif one['type'] == 'dir':
                    return_dirs.append({
                        "path": create_path(one["path"], i.path),
                        "format": one["format"],
                        "description": one["description"],
                        "size": one["size"]
                    })
                else:
                    raise Exception('错误的文件类型')
        return return_files, return_dirs

    def get_sheet(self):
        """
        获取sheet数据pk路径
        """
        self._sheet_pk = sys.argv[1]
        data = pickle.load(open(self._sheet_pk, 'rb'))
        return Sheet(data=data)


if __name__ == '__main__':
    worflow_object = Instant()
    worflow_object.run()
