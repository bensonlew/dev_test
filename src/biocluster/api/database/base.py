# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.config import Config
import importlib
import functools
from biocluster.core.function import get_clsname_form_path


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
        class_name = get_clsname_form_path(name, tp="")
        module = importlib.import_module("mbio.api.database.%s" % name.lower())
        lib_obj = getattr(module, class_name)(self._bind_object)
        return lib_obj


def report_check(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if args[0].bind_object.IMPORT_REPORT_DATA is not True:
            return False
        else:
            return f(*args, **kwargs)
    return wrapper
