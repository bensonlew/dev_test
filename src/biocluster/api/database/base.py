# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.config import Config
from pymongo import MongoClient
import importlib


class Base(object):
    def __init__(self, bind_object):
        self._bind_object = bind_object
        self._config = Config()
        self._client = MongoClient(self._config.MONGO_URI)
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


class ApiManager(object):
    def __init__(self, bind_object):
        self._bind_object = bind_object
        self._api_dict = {}

    def __getattr__(self, name):
        if name not in self._api_dict.keys():
            self._api_dict[name] = self.get_api(name)
        return self._api_dict[name]

    def get_api(self, name):
        """
        获取api对象

        :param name:
        :return:
        """
        module = importlib.import_module("mbio.api.database.%s" % name.lower())
        lib_obj = getattr(module, name.capitalize())(self._bind_object)
        return lib_obj
