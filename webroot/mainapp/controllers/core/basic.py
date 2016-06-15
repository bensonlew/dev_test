# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import importlib
import errno
import time
import datetime
import random
import web
import re
import inspect
import shutil
import json
from biocluster.logger import Wlog
from biocluster.config import Config as mainConfig
from mainapp.models.instant_task import InstantTask
from mainapp.models.mongo.public.meta.meta import Meta
from mainapp.config.db import Config
from mainapp.models.mongo.core.base import ApiManager


class Basic(object):
    def __init__(self):
        self.db = Config().get_db()
        self._id = ""
        self.data = None
        self._name = self.__get_min_name()
        self._instantModule = None
        self._work_dir = ""
        self._memberId = ""
        self._taskId = ""
        self._projectSn = ""
        self._client = ""
        self._output_dir = ""
        self._uploadTarget = ""
        self.logger = None
        self._options = dict()
        self._uploadDirObj = list()
        self.api = ApiManager(self)
        self._hasUploaded = False
        info = {"success": False, "info": "程序非正常结束"}
        self.returnInfo = json.dumps(info)

    def POST(self):
        data = web.input()
        self.data = data
        self._client = data.client
        if hasattr(data, "taskId"):
            otuId = None
            taskId = data.taskId
            self._taskId = taskId
        else:
            if hasattr(data, "otu_id"):
                otuId = data.otu_id
                otu_info = Meta().get_otu_table_info(data.otu_id)
                taskId = otu_info["task_id"]
                self._taskId = taskId
                self._projectSn = otu_info["project_sn"]
                task_info = Meta().get_task_info(otu_info["task_id"])
                self._memberId = task_info["member_id"]

        if data.taskType == "projectTask":
            self._id = data.taskId
        elif data.taskType == "reportTask":
            self._id = self.GetNewId(taskId, otuId)
        self._work_dir = Config().get_work_dir()
        timestr = str(time.strftime('%Y%m%d', time.localtime(time.time())))
        self._work_dir = self._work_dir + "/" + timestr + "/" + self.name + "_" + self._id
        self._output_dir = self._work_dir + "/" + "output"
        self.create_work_dir()
        self.logger = Wlog(self).get_logger(self.name + "(" + self.id + ")")
        self.addStartRecord()

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
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        insertData = {
            "client": client,
            "taskId": self.id,
            "json": json.dumps(data),
            "ip": web.ctx.ip,
            "addTime": datetime.datetime.now(),
            "workdir": self.work_dir
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

    def run(self):
        self._instantModule.run()

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    def __get_min_name(self):
        class_name = self.__class__.__name__
        return class_name

    @property
    def work_dir(self):
        return self._work_dir

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
        self._uploadTarget = targetPath

    @property
    def output_dir(self):
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value):
        if not os.path.isdir(value):
            raise Exception("目录{}不存在，请确认!".format(value))
        else:
            self._output_path = value

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, optionDict):
        if not isinstance(optionDict, dict):
            raise Exception("optionDict 不是一个字典")
        else:
            self._options = optionDict

    def setOptions(self, optionDict):
        if not isinstance(optionDict, dict):
            raise Exception("optionDict 不是一个字典")
        else:
            self._options = optionDict

    def create_work_dir(self):
        """
        建立工作路径
        """
        dir_list = [self.work_dir, self.output_dir]
        for name in dir_list:
            try:
                os.makedirs(name)
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(name):
                    pass
                else:
                    raise OSError("创建目录{}失败".format(name))

    @property
    def instantModule(self):
        return self._instantModule

    def importInstant(self, project):
        """
        从mbio.package中动态导入instant包
        """
        myModule = inspect.stack()[1][0]
        fileName = inspect.getmodule(myModule).__name__
        moduleName = fileName
        moduleName = re.split("\.", moduleName).pop()
        moduleName = "mbio.packages.instant." + project + "." + moduleName
        name = re.split("\.", moduleName).pop()
        l = name.split("_")
        l.append("instant")
        l = [el.capitalize() for el in l]
        className = "".join(l)
        imp = importlib.import_module(moduleName)
        self._instantModule = getattr(imp, className)(self)
        return self._instantModule

    def add_upload_dir(self, dirPath):
        """
        添加需要上传的目录

        :param dirPath: 相对或绝对路径
        :return: UploadDir对象
        """
        if not os.path.isdir(dirPath):
            raise Exception("上传路径{}不是一个文件夹".format(dirPath))
        relPath = os.path.relpath(dirPath, self.work_dir)
        m = re.match("^\.", relPath)
        if m:
            raise Exception("{}不是当前工作目录的子目录".format(dirPath))
        for i in self._uploadDirObj:
            if i.uploadPath == relPath:
                raise Exception("不能重复添加目录{}!".format(dirPath))
        up = UploadDir(self)
        up.path = dirPath
        up.uploadPath = relPath
        self._uploadDirObj.append(up)
        return up

    def uploadFiles(self, name=None):
        """
        生成上传目标的文件路径
        上传文件到目标路径
        """
        if self._hasUploaded:
            raise Exception("文件已经上传！")
        if self._uploadTarget != "":
            if name is not None:
                raise Exception("上传的目的路径已经被指定！")
            else:
                self._execUpload()
        else:
            self._createUploadTarget(name)
            self._execUpload()
        self._hasUploaded = True

    def _execUpload(self):
        """
        上传文件到目标路径
        """
        if len(self._uploadDirObj) == 0:
            self.logger.warning("还没有需要上传的文件夹，停止上传！")
        for upObj in self._uploadDirObj:
            if not os.path.exists(upObj.path):
                os.makedirs(upObj.path)
            if os.path.isdir(upObj.path):
                basename = os.path.basename(upObj.path)
                target = os.path.join(self._uploadTarget, basename)
                if os.path.exists(target):
                    flag = 1
                    while flag:
                        try:
                            shutil.rmtree(target)
                        except Exception:
                            time.sleep(1)
                        else:
                            flag = 0
                try:
                    self.logger.info("开始上传文件夹{}到{}".format(upObj.path, target))
                    self._copyDir(upObj.path, target)
                except Exception as e:
                    self.logger.info("文件夹{}上传出错:{}".format(upObj.path, e))
                else:
                    self.logger.info("文件夹{}上传完成".format(upObj.path))
            else:
                raise Exception("暂时不支持单个文件的上传！")

    def _copyDir(self, src, dst, symlinks=False, ignore=None):
        if not os.path.exists(dst):
            os.makedirs(dst)
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                self._copyDir(s, d, symlinks, ignore)
            else:
                shutil.copy2(s, d)

    def _createUploadTarget(self, name):
        """
        生成上传的目标路径，当memberId, projectSn, name未知的时候，会用unknown代替
        """
        if self._memberId == "":
            memberId = "unknownMemberId"
        else:
            memberId = self._memberId
        if self._projectSn == "":
            projectSn = "unknownProjectSn"
        else:
            projectSn = self._projectSn
        if name is None:
            name = "unknownName"
        if self._client == "client01":
            typeName = "sanger"
        elif self._client == "client03":
            typeName = "tsanger"
        else:
            raise Exception("未识别的client")
        pathConfig = mainConfig().get_netdata_config(typeName)
        pathPre = pathConfig[typeName + "_path"]
        strTime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._uploadTarget = "rerewrweset/files/{}/{}/{}/report_results/{}_{}".format(memberId, projectSn, self.taskId, name, strTime)
        self._uploadTarget = os.path.join(pathPre, self._uploadTarget)

    @property
    def upload_dir(self):
        return self._uploadDirObj

    def end(self):
        content = dict()
        content["dirs"] = list()
        content["files"] = list()
        for obj in self._uploadDirObj:
            (f, d) = obj.fileListForReturn()
            for myf in f:
                content["files"].append(myf)
            for myd in d:
                content["dirs"].append(myd)
        info = dict()
        info["success"] = True
        info["content"] = content
        self.returnInfo = json.dumps(info)
        self.addEndRecord()


class UploadDir(object):
    """
    需要远程上传的结果信息文件格式和信息
    """
    def __init__(self, bindObject):
        self._dirPath = ""
        self._fileList = list()
        self._bindObject = bindObject
        self._regexpRules = list()
        self._relpathRules = list()
        self.uploadPath = ""
        self._hasAdd = False

    @property
    def path(self):
        return self._dirPath

    @path.setter
    def path(self, dirPath):
        """
        设置需要上传的文件夹路径，将上传所有子文件和文件夹，这个路径通常为工作目录下的output文件夹
        """
        if not os.path.isdir(dirPath):
            raise Exception("{}路径不是有效的文件夹路径".format(dirPath))
        else:
            self._dirPath = os.path.abspath(dirPath)
            if not os.listdir(dirPath):
                self._bindObject.logger.warning("文件夹{}为空，请确认结果文件是否已经拷贝？".format(dirPath))

    def add_regexp_rules(self, matchRules):
        """
        使用相对于当前添加的上传文件夹的相对路径添加正则匹配规则

        :param match_rules: 必须为一个二维数组, 每个子数组含有3个字符串元素，第一个元素为正则表达式，
        第二个元素为格式path, 第三个元素为文件或文件夹说明
        """
        if not isinstance(matchRules, list):
            raise Exception("匹配规则必须为数组!")
        for rule in matchRules:
            self._regexpRules.append(rule)

    def add_relpath_rules(self, matchRules):
        """
        添加路径匹配，使用相对于当前添加的上传文件夹的相对路径匹配，当前文件夹使用“.”，匹配
        :param match_rules:必须为一个二维数组, 每个子数组含有3个字符串元素，第一个元素为相对路径，
        第二个元素为格式path, 第三个元素为文件或文件夹说明
        """
        if not isinstance(matchRules, list):
            raise Exception("匹配规则必须为数组!")
        for rule in matchRules:
            self._relpathRules.append(rule)

    def match(self):
        """
        根据添加的regexp_rules和relpath_rules匹配所有文件和文件夹，如果regexp_rules和relpath_rules有冲突，
        relpath_rules生效，正则有冲突，后添加的规则生效
        """
        self._hasAdd = True
        for i in os.walk(self._dirPath):
            self._fileList.append(ResultFile(i[0], self._dirPath, "dir"))
            for fileName in i[2]:
                self._fileList.append(ResultFile(os.path.join(i[0], fileName), self._dirPath, "file"))
        for rRule in self._regexpRules:
            pattern = re.compile(rRule[0])
            for subFile in self._fileList:
                match = pattern.match(subFile.relpath)
                if match:
                    subFile.format = rRule[1]
                    subFile.description = rRule[2]
        for rRule in self._relpathRules:
            for subFile in self._fileList:
                if os.path.relpath(subFile.relpath, rRule[0]) == ".":
                    subFile.format = rRule[1]
                    subFile.description = rRule[2]

        for subFile in self._fileList:
            if subFile.fileType == "file" and subFile.format == "":
                self._bindObject.logger.warning("文件{}没有设置格式，确认此文件真的无法确认格式？".format(subFile.fullPath))

    @property
    def fileList(self):
        """
        文件对象列表

        :return: 数组，数组元素为ResultFile对象
        """
        if not self._hasAdd:
            self.match()
        data = list()
        for i in self._fileList:
            data.append({
                "path": i.relpath,
                "type": i.fileType,
                "format": i.format,
                "description": i.description,
                "size": i.size
            })
        return data

    def fileListForReturn(self):
        if self._bindObject._uploadTarget == "":
            raise Exception("文件还未上传， 无法获取返回json格式")
        files = list()
        dirs = list()
        for l in self.fileList:
            if l["type"] == "file":
                files.append({
                    "path": os.path.join(self._bindObject._uploadTarget, os.path.basename(self._dirPath), l["path"]),
                    "format": l["format"],
                    "description": l["description"],
                    "size": l["size"]
                })
            elif l["type"] == "dir":
                tmpPath = os.path.join(self._bindObject._uploadTarget, os.path.basename(self._dirPath), l["path"])
                tmpPath = re.sub("\.$", "", tmpPath)
                dirs.append({
                    "path": tmpPath,
                    "format": l["format"],
                    "description": l["description"],
                    "size": l["size"]
                })
        return (files, dirs)


class ResultFile(object):
    """
    保持单个结果文件的信息
    """
    def __init__(self, fullPath, basePath, fileType="file"):
        self.fileType = fileType
        self.fullPath = fullPath
        self.basePath = basePath
        self.relpath = os.path.relpath(fullPath, basePath)
        self.format = ""
        self.description = ""

    @property
    def size(self):
        if self.fileType == "file":
            return os.path.getsize(self.fullPath)
        else:
            return ""
