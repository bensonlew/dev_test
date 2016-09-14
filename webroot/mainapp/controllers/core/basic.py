# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# lastmodified:20160701 by shenghe
import datetime
import random
import json
import web
import re
import os
import time
import pickle
import subprocess
from mainapp.models.instant_task import InstantTask
from biocluster.config import Config as mbio_config
from biocluster.core.function import get_clsname_form_path


class Basic(object):
    def __init__(self):
        self._mainTableId = ""  # 核心表在mongo的id，如otu表的id
        self.config = mbio_config()
        self._id = ""  # 新的ID
        self.data = None  # web数据
        self._name = self.__get_min_name()  # 实例类名称
        self._task_object_name = ''  # 任务的对象名称
        self._taskId = ""
        self._projectSn = ""
        self._client = ""
        self._uploadTarget = ""  # 文件上传路径
        self.logger = None  # 必须在run开始后，logger为workflow的logger
        self._options = dict()
        self.task_name = ''  # 需要调用的workflow或者module或者tool的路径(目前只支持workflow)，如: meta.report.distance_calc
        self.params = ""  # 在controller里面设定，这个值会在后续中写到mongo的主表当中去
        self.task_type = ''  # 调用的类型workflow或者module或者tool
        self._task_object = None  # 用于存储调用的task对象
        self.to_file = []  # 使用to_file模块，同原有写法
        self.USE_DB = False  # 是否使用数据库， 不一定生效，与运行的workflow是否设置使用rpc有关
        self._sheet = None  # 存放Sheet对象
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
        self._sheet = sheet_data
        self.pickle_sheet()
        return self._sheet

    def __work_dir(self):
        """
        获取并创建工作目录
        """
        work_dir = self.config.WORK_DIR
        timestr = str(time.strftime('%Y%m%d', time.localtime(time.time())))
        work_dir = work_dir + "/" + timestr + "/" + self._task_object_name + "_" + self._id
        return work_dir

    def create_work_dir(self):
        """
        建立工作目录

        :return:
        """
        if not os.path.exists(self._work_dir):
            os.makedirs(self._work_dir)

    def pickle_sheet(self):
        """
        打包sheet 字典为pk文件

        :return:
        """
        self._task_object_name = self.__get_task_object_name()
        self._work_dir = self.__work_dir()
        self.create_work_dir()
        self._pk_sheet = self._work_dir + '/' + self._task_object_name + '_sheet.pk'
        with open(self._pk_sheet, 'w') as w:
            pickle.dump(self._sheet, w)



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

    def run(self):
        """
        运行即时计算，分三步，一、根据参数生成Sheet对象；二、获取workflow类对象，并使用Sheet对象实例化，运行，三、处理运行结果
        """
        self.create_sheet()
        try:
            bin_dir = os.path.dirname(self.config.WORK_DIR) + '/biocluster/bin'
            cmd = 'python {}/run_instant.py {}'.format(bin_dir, self._pk_sheet)
            # print 'INSTANT CMD:', cmd
            subprocess.check_output(cmd, shell=True)
        except Exception as e:
            print 'run_instant计算出错：', e
            info = {"success": False, "info": "计算程序计算错误"}
            self.returnInfo = json.dumps(info)
            return self.returnInfo
        self._return_pk = self._work_dir + '/' + 'return_web.pk'
        if not os.path.exists(self._return_pk):
            info = {"success": False, "info": "计算程序异常结束"}
            self.returnInfo = json.dumps(info)
            return self.returnInfo
        self.returnInfo = json.dumps(pickle.load(open(self._return_pk, 'rb')))
        self.addEndRecord()
        # print 'WEB ReturnInfo', self.returnInfo

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

    def __get_min_name(self):
        class_name = self.__class__.__name__
        return class_name

    def __get_task_object_name(self):
        """
        获取task对象的名称
        """
        class_name = get_clsname_form_path(self.task_name, tp=self.task_type.capitalize())
        class_name = class_name.replace(self.task_type.capitalize(), '')
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
