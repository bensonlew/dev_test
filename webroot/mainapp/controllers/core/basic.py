# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# lastmodified:20160701 by shenghe
import importlib
import datetime
import random
import json
import web
import re
import os
import threading
from mainapp.models.instant_task import InstantTask
from mainapp.config.db import Config, get_mongo_client
from biocluster.wsheet import Sheet
from mbio.api.database.meta_update_status import MetaUpdateStatus  # 暂时使用这个更新，最好在基类base中加一个函数方法
from biocluster.config import Config as mbio_config
from bson import ObjectId


class Basic(object):
    def __init__(self):
        self.mongodb = get_mongo_client()  # mongo库
        self.db = Config().get_db()  # mysql库
        self._mainTableId = ""  # 核心表在mongo的id，如otu表的id
        self._id = ""  # 新的ID
        self.data = None  # web数据
        self._name = self.__get_min_name()  # 实例类名称
        self._taskId = ""
        self._projectSn = ""
        self._client = ""
        self._uploadTarget = ""  # 文件上传路径
        self.logger = None  # 必须在run开始后，logger为workflow的logger
        self._options = dict()
        self._uploadDirObj = list()
        self.task_name = ''  # 需要调用的workflow或者module或者tool的路径(目前只支持workflow)，如: meta.report.distance_calc
        self.params = ""  # 在controller里面设定，这个值会在后续中写到mongo的主表当中去
        self.task_type = ''  # 调用的类型workflow或者module或者tool
        self._task_object = None  # 用于存储调用的task对象
        self.to_file = []  # 使用to_file模块，同原有写法
        self.USE_DB = False  # 是否使用数据库， 不一定生效，与运行的workflow是否设置使用rpc有关
        self._sheet = None  # 存放Sheet对象
        self._mongo_ids = []  # 存放worflow返回的写入mongo表的信息，每条信息为一个字典，含有collection_name,id,desc三个字段
        self.update_api = None  # 存放更新sg_status的方法
        info = {"success": False, "info": "程序非正常结束(没有获取到有关错误信息)"}
        self.returnInfo = json.dumps(info)
        self.IMPORT_REPORT_AFTER_END = False

    def POST(self):
        if self._taskId == "" or self._memberId == "" or self._projectSn == "":
            info = {"success": False, "info": "没有获取到正确的taskId,memberId,projectSn"}
            self.returnInfo = json.dumps(info)
            return self.returnInfo
        self._id = self.GetNewId(self._taskId, self._mainTableId)
        self.addStartRecord()

    def create_sheet(self):
        """用于生成实例化workflow的sheet"""
        if not self.task_type:
            self.task_type = 'workflow'
        if not self.task_name or not self._client or not self.options:
            return False
        sheet_data = {
            'id': self.id,
            'stage_id': 0,
            'name': self.task_name,  # 需要配置
            'type': self.task_type,  # 可以配置
            'client': self._client,
            'project_sn': self.projectSn,
            'USE_DB': self.USE_DB,
            'IMPORT_REPORT_DATA': True,
            'USE_RPC': False,
            'params': self.params,
            'instant': True,
            'options': self.options  # 需要配置
        }
        if self.to_file:  # 可以配置
                sheet_data['to_file'] = self.to_file
        if not self.uploadTarget:
            self._uploadTarget = self._createUploadTarget()
        sheet_data['output'] = self._uploadTarget
        self._sheet = Sheet(data=sheet_data)
        return self._sheet

    def _createUploadTarget(self):
        """
        根据client来确定需要上传文件的位置

        return str 上传路径
        """
        if self._client == 'client01':
            self._uploadTarget = 'sanger'
        elif self._client == 'client03':
            self._uploadTarget = 'tsanger'
        else:
            raise Exception('未识别的client:{}'.format(self._client))
        self._uploadTarget = self._uploadTarget + ":rerewrweset/files/" + str(self.memberId) + '/' + str(self.projectSn) + "/" + str(self.taskId)
        self._uploadTarget = self._uploadTarget + "/report_results/" + self.name + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        return self._uploadTarget

    def _run(self):
        """
        运行即时计算，分三步，一、根据参数生成Sheet对象；二、获取workflow类对象，并使用Sheet对象实例化，运行，三、处理运行结果
        """
        try:
            self.create_sheet()
            self.get_task_object()
            self.logger = self._task_object.logger
            self._task_object.run()
        except Exception as e:
            print e
            info = {"success": False, "info": "{}".format(e)}
            self.returnInfo = json.dumps(info)
            # self.logger.error(self.returnInfo)
            return self.returnInfo
        self._mongo_ids = self._task_object.return_mongo_ids
        self.update_api = MetaUpdateStatus(self._task_object)
        self.update_api.manager = self._task_object.api
        self._uploadDirObj = self._task_object._upload_dir_obj
        self.end()

    def run(self):
        """新线程运行_run方法"""
        print('即时计算 Thread start run......')
        run_object = threading.Thread(target=self._run)
        run_object.start()
        run_object.join()
        print('即时计算 Thread over......')

    def get_task_object(self, origin='mbio'):
        """"""
        path = origin + '.' + self.task_type + 's.' + self.task_name
        module = importlib.import_module(path)
        module_name = self.task_name.split('.')[-1]
        class_name = [i.capitalize() for i in module_name.split('_')]
        class_name.append(self.task_type.capitalize())
        class_name = ''.join(class_name)
        task_class = getattr(module, class_name)
        if isinstance(self._sheet, Sheet):
            self._task_object = task_class(self._sheet)
            return self._task_object
        else:
            raise Exception('在调用get_task_object之前，需要先调用create_sheet')

    def GetNewId(self, taskId, otuId=None):
        if otuId:
            newId = "{}_{}_{}".format(taskId, otuId[-4:], random.randint(1, 10000))
        else:
            newId = "{}_{}_{}".format(taskId, random.randint(1000, 10000), random.randint(1, 10000))
        iTask = InstantTask()
        iData = iTask.GetByTaskId(newId)
        if len(iData) > 0:
            return self.GetNewId(taskId, otuId)
        return newId

    def addStartRecord(self):
        """
        当一个任务即将开始运行的时候，往instantTask表里添加一条记录
        """
        data = web.input()
        self._client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        insertData = {
            "client": self._client,
            "taskId": self.id,
            "json": json.dumps(self.data),
            # "ip": web.ctx.ip,
            "addTime": datetime.datetime.now(),
            # "workdir": self._task_object.workdir
        }
        iTask = InstantTask()
        iTask.AddRecord(insertData)

    def addEndRecord(self):
        """
        当一个任务即将结束的时候，将instantTask表里的记录进行更新
        """
        updateData = dict()
        updateData["endTime"] = str(datetime.datetime.now())
        updateData["returnJson"] = self.returnInfo
        iTask = InstantTask()
        iTask.UpdateRecord(self.id, updateData)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def mongo_ids(self):
        return self._mongo_ids

    def __get_min_name(self):
        class_name = self.__class__.__name__
        return class_name

    @property
    def memberId(self):
        return self._memberId

    @property
    def projectSn(self):
        return self._projectSn

    @property
    def taskId(self):
        return self._taskId

    @property
    def uploadTarget(self):
        return self._uploadTarget

    @uploadTarget.setter
    def uploadTarget(self, targetPath):
        if not re.match(r'^sanger\:rerewrweset\/files\/', targetPath):
            raise Exception('暂不支持或者错误的上传路径')
        self._uploadTarget = targetPath

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, optionDict):
        if not isinstance(optionDict, dict):
            raise Exception("optionDict 不是一个字典")
        else:
            self._options = optionDict

    def set_options(self, optionDict):
        if not isinstance(optionDict, dict):
            raise Exception("optionDict 不是一个字典")
        else:
            self._options = optionDict

    @property
    def upload_dirs(self):
        return self._uploadDirObj

    def update_sg_status(self):
        """更新sg_status表"""
        if self.update_api:
            for one_insert in self.mongo_ids:
                return_id = self.update_api.add_meta_status(table_id=one_insert['id'],
                                                            type_name=one_insert['collection_name'],
                                                            desc=one_insert['desc'], task_id=self.taskId)
                print('sg_status_ID:', return_id)

    def end(self):
        self.update_sg_status()
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
        self.returnInfo = json.dumps(info)
        self.addEndRecord()

    def get_upload_files(self):
        """将workflow的文件上传对象的文件列表取出，用于返回前端"""
        return_files = []
        return_dirs = []

        def create_path(path, dir_path):
            dir_path = os.path.split(dir_path)[1]
            return self.uploadTarget + '/' + dir_path + '/' + path.lstrip('.')
        for i in self.upload_dirs:
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

    def get_main_table_name(self, table, main_id):
        """
        查询数据库获取主表名称name
        """
        table_name = self.mongodb[mbio_config().MONGODB][table].find_one({'_id': ObjectId(main_id)})['name']
        if not table_name:
            raise Exception('在表:{} 中未找到_id为:{} 的数据，或者表中没有"name"字段'.format(table, main_id))
        return table_name
