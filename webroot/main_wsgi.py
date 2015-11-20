# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipline import Pipline

# web.config.debug = False
urls = (
    "/hello", "hello",
    "/pipline", "Pipline"
    )


class hello(object):
    @check_sig
    def GET(self):
        return "xxxx"


application = web.application(urls, globals()).wsgifunc()
