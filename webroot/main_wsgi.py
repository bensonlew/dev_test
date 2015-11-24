# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipline import Pipline
from mainapp.controllers.filecheck import FileCheck

# web.config.debug = False
urls = (
    "/hello", "hello",
    "/filecheck", "FileCheck",
    "/pipline", "Pipline"
    )


class hello(object):
    @check_sig
    def GET(self):
        return "zzz"


application = web.application(urls, globals()).wsgifunc()
