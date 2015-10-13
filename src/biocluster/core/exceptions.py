# -*- coding: utf-8 -*-
# __author__ = 'guoquan'


class EventStopError(Exception):
    """
    事件已停止异常，发生此异常时是由于事件已经停止监听或尚未启动监听时触发此事件
    """
    def __init__(self, name):
        Exception.__init__(self)
        self.name = name


class UnknownEventError(Exception):
    """
    事件未定义异常，发生此异常时是由于事件尚未定义时触发此事件
    """

    def __init__(self, name):
        Exception.__init__(self)
        self.name = name


class OptionError(Exception):
    """
    参数错误
    """

    def __init__(self, name):
        Exception.__init__(self)
        self.name = name


class FileError(Exception):
    """
    参数错误
    """

    def __init__(self, name):
        Exception.__init__(self)
        self.name = name