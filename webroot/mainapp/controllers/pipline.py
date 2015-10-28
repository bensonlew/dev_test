# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig


class Pipline(object):

    @check_sig
    def POST(self):
        data = web.data()