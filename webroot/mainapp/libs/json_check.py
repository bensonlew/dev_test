# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import functools
import json
import web


def check_json(f):
    @functools.wraps(f)
    def wrapper(obj):
        data = web.input()
        if (not hasattr(data, "json")) or len(data.json) < 1:
            raise web.badrequest(u"Json cannot be null!")
        try:
            json.loads(data.json)
            return f(obj)
        except ValueError:
            raise web.badrequest(u"Json format not correct!")
    return wrapper
