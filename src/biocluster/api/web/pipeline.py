# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from gevent import monkey; monkey.patch_all()
import urllib2
import sys
from biocluster.config import Config

class Pipeline(object):
    def __init__(self, obj=None):
        self._obj = obj
        self._paramas = None

    def set_paramas(self, params):

        self._paramas = params

    def update(self, data):

        mod = sys.modules['__main__']



class Sanger(object):
    def __init__(self):
        self._update_url = ""
