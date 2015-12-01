# encoding: utf-8
# __author__ = 'yuguo'

"""参数类"""

from .iofile import FileBase
from .core.function import load_class_by_path
import os
from .core.exceptions import OptionError
import re


class Option(object):
    """
    参数选项的属性库设定
    """
    def __init__(self, opt):
        """
        初始化一个参数
        参数使用
        """
        self._options = opt
        self._format = None
        self._check = None
        if not isinstance(opt, dict):
            raise OptionError("opt必须为一个字典")
        for attr in ('name', 'type'):
            if attr not in opt.keys():
                raise OptionError("必须设置参数属性 {}".format(attr))
        if opt['type'] in {'outfile', 'infile'}:
            if 'format' not in opt.keys():
                raise OptionError("必须设置参数属性 format")
            else:
                # formats = re.split('\s*,\s*', opt['format'])
                self._format = opt['format']
                self._check = opt['check'] if 'check' in opt.keys() else False
                self._value = load_class_by_path(self._format, "File")()
        else:
            self._value = opt['default'] if 'default' in opt.keys() else False

        if opt['type'] not in {"int", "float", "string", "bool", "infile", "outfile"}:
            raise OptionError("参数属性不在规范范围内type：{}".format(self._type))

        self._name = opt['name']
        self._type = opt['type']
        self._is_set = False

    @property
    def name(self):
        """
        获取参数名

        :return: string
        """
        return self._name

    @property
    def type(self):
        """
        获取参数属性

        :return: String
        """
        return self._type

    @property
    def value(self):
        """
        获取参数值

        也可直接赋值,当参数为文件类参数时自动判断，当value为实际存在的文件路径时，调用set_path设置File对象路径。
        当value为File文件对象时，检测是否与自身文件类文件格式类型相同，如果相同则替换,不相同时报错

        :return:
        """
        return self._value

    @value.setter
    def value(self, value):
        """


        :param value:
        :return:
        """
        if self._type in {'outfile', 'infile'}:
            if isinstance(value, unicode) or isinstance(value, str):
                if os.path.exists(value):
                    self._value.set_path(value)
                else:
                    raise OptionError("文件不存在！")
            else:
                self._check_type(value)
                class_obj = load_class_by_path(self._format, "File")
                if isinstance(value, class_obj):
                    self._value = value
                else:
                    raise OptionError("设置的文件对象格式不匹配")
        else:
            self._check_type(value)
            self._value = value

    @property
    def format(self):
        """
        获取文件类型参数
        """
        return self._format

    @property
    def check(self):
        """
        获取文件类型参数
        """
        return self._check

    @property
    def is_set(self):
        """
        返回参数对象是否被设置了值
        :return:
        """
        return self._is_set

    def _check_type(self, value):
        """
        检测值是否符合要求
        :param value:
        :return:
        """
        if self._type == "int":
            if not isinstance(value, int):
                raise OptionError("参数值类型不符合{}:{}".format(self._type, value))
        if self._type == "float":
            if not isinstance(value, float):
                raise OptionError("参数值类型不符合{}:{}".format(self._type, value))
        if self._type == "bool":
            if not isinstance(value, bool):
                raise OptionError("参数值类型不符合{}:{}".format(self._type, value))
        if self._type == "string":
            if not (isinstance(value, unicode) or isinstance(value, str)):
                raise OptionError("参数值类型不符合{}:{}".format(self._type, value))
        if self._type in {"infile", "outfile"}:
            if not isinstance(value, FileBase):
                raise OptionError("参数值类型不符合{}:{}".format(self._type, value))
