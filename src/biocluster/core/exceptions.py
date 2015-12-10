# -*- coding: utf-8 -*-
# __author__ = 'guoquan'


class Error(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)
        self.value = value

    def __str__(self):
        return str(self.value)


class EventStopError(Error):
    """
    事件已停止异常，发生此异常时是由于事件已经停止监听或尚未启动监听时触发此事件
    """
    pass


class UnknownEventError(Error):
    """
    事件未定义异常，发生此异常时是由于事件尚未定义时触发此事件
    """

    pass


class OptionError(Error):
    """
    参数错误
    """

    pass


class FileError(Error):
    """
    参数错误
    """
    pass
