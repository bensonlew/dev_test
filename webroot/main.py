# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipline import Pipline, PiplineState,PiplineLog,PiplineStop
from mainapp.controllers.filecheck import FileCheck

#web.config.debug = False
urls = (
    "/hello", "hello",
    "/filecheck", "FileCheck",
    "/pipline", "Pipline",
    "/pipline/state", "PiplineState",
    "/pipline/log", "PiplineLog",
    "/pipline/stop", "PiplineStop",
    )


class hello(object):
    @check_sig
    def GET(self):
        return "xxxx"

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()