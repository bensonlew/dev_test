# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import gevent
# from gevent import monkey; monkey.patch_socket()
import sys
from biocluster.config import Config


class Pipeline(object):
    def __init__(self):
        self._api_obj = None
        self._client = None
        self._database_mode = False
        self._data = None

    def update(self, data):
        self._data = data

    def save_to_database(self, api, data):
        pass

    @property
    def api(self):
        if not self._api_obj:
            if self._obj:
                client = self._obj.get_workflow().sheet['client']
            else:
                client = self._client
            type_name = Config().get_api_type(client)
            mod = sys.modules['__main__']
            self._api_obj = getattr(mod, type_name)()
        return self._api_obj


class Sanger(object):
    def __init__(self):
        self._update_url = ""
        self._paramas = None

    def set_paramas(self, params):
        self._paramas = params
