# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import importlib
import json
from collections import OrderedDict
from biocluster.config import Config


class Base(object):
    def __init__(self, bind_object):
        self._bind_object = bind_object
        self._config = Config()
        self._client = self._config.mongo_client
        self._db_name = "test"
        self._db = None

    @property
    def bind_object(self):
        return self._bind_object

    @property
    def db(self):
        if self._db is None:
            self._db = self._client[self._db_name]
        return self._db

    @staticmethod
    def SortDict(param):
        if not isinstance(param, dict):
            raise Exception("传入的param不是一个字典")
        new_param = OrderedDict(sorted(param.items()))
        params = json.dumps(new_param)
        params = re.sub(':\s+', ':', params)
        params = re.sub(',\s+', ',', params)
        return params


class ApiManager(object):
    """
    api对象， 用于管理进行mongo数据库导入的各个api
    """
    def __init__(self, bind_object):
        self._bind_object = bind_object
        self._api_dict = dict()

    def __getattr__(self, name):
        if name not in self._api_dict.keys():
            self._api_dict[name] = self._get_api(name)
        return self._api_dict[name]

    def api(self, name):
        if name not in self._api_dict.keys():
            self._api_dict[name] = self._get_api(name)
        return self._api_dict[name]

    def _get_api(self, name):
        """
        用名字获取api对象, 名字应该从mainapp/models/mongo/instant 下一级开始， 比如名字为
        meta.pan_core, 则模块的路径为mainapp/models/mongo/instant/meta/pan_core, 类名为PanCoreMongo
        """
        moduleName = "mainapp.models.mongo.instant." + name
        name = re.split("\.", name)
        className = name.pop(-1)
        l = className.split("_")
        l.append("Mongo")
        l = [el.capitalize() for el in l]
        className = "".join(l)
        print moduleName
        imp = importlib.import_module(moduleName)
        obj = getattr(imp, className)(self._bind_object)
        return obj
