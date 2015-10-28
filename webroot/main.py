# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig

web.config.debug = False
urls = ("/hello", "hello")


class hello(object):
    @check_sig
    def GET(self):
        return "xxxx"

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()