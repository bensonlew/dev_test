# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""单例模式"""


def singleton(cls, *args, **kw):
    """
    定义单例模式
    """
    instances = {}

    def _singleton(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

