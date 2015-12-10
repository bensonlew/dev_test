# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""工作流表单"""

import json


class Sheet(object):
    """
    workflow 表单
    """
    def __init__(self, jsonfile="", data=None):
        """
        根据配置文件或者配置对象生成Sheet对象
        :param jsonfile: file path
        :param data:  Python object convert from json file,未指定jsonfile时生效
        :return:
        """
        self._data = {}
        if jsonfile:
            with open(jsonfile, 'r') as f:
                self._data = json.load(f)
        else:
            self._data = data

    @property
    def id(self):
        """
        任务ID
        """
        return self._data['id']

    @property
    def name(self):
        """
        模块名称
        """
        if 'name' in self._data.keys():
            return self._data['name']
        else:
            return "link"

    @property
    def type(self):
        """
        调用类型
        """
        if 'type' in self._data.keys():
            return self._data['type']
        else:
            return "link"

    @property
    def output(self):
        """
        需要上传的远程路径
        :return:
        """
        if 'output' in self._data.keys():
            return self._data['output']
        else:
            return None

    def option(self, name, component=None):
        """
        获取参数值
        """

        if self.type == "link":
            if component not in self._data['components'].keys():
                raise Exception("列表中没有components:%s" % component)
            data = self._data['components'][component]['options']
        else:
            data = self._data['options']
        if name not in data.keys():
            raise Exception("没有参数%s" % name)
        return data[name]

    def options(self, component=None):
        """
        获取所有Option
        :return: dict name/value
        """
        if self.type == "link":
            if component not in self._data['components'].keys():
                raise Exception("列表中没有components:%s" % component)
            data = self._data['components'][component]['options']
        else:
            data = self._data['options']
        return data

