# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import web
import mainapp.core.auto_load
urls = (
    "/hello", "hello"
)


class hello(object):
    def GET(self):
        return "hello"


if __name__ == "__main__":
    app = web.application(urls, globals())
    mainapp.core.auto_load.register(app)
    app.run()
