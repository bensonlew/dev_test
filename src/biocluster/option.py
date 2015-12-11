# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""参数类"""

from .iofile import FileBase
from .core.function import load_class_by_path, get_classpath_by_object
import os
from .core.exceptions import OptionError
import re
import traceback
from .core.exceptions import FileError


class Option(object):
    """
    参数选项的属性库设定
    """
    def __init__(self, opt, bind_obj=None):
        """
        初始化一个参数
        参数使用
        """
        self._options = opt
        self._format = None
        self._check = None
        self._format_list = []
        self._check_list = []
        self.bind_obj = bind_obj
        if not isinstance(opt, dict):
            raise OptionError("opt必须为一个字典")
        for attr in ('name', 'type'):
            if attr not in opt.keys():
                raise OptionError("必须设置参数属性 {}".format(attr))
        if opt['type'] in {'outfile', 'infile'}:
            if 'format' not in opt.keys():
                raise OptionError("必须设置参数属性 format")
            else:
                formats = re.split('\s*,\s*', opt['format'])
                if len(formats) > 1:
                    if 'check' in opt.keys():
                        checks = re.split('\s*,\s*', opt['check'])
                        if len(checks) == 1:
                            for index in range(len(formats)):
                                self._format_list.append(formats[index].strip())
                                self._check_list.append(checks[0].strip())
                        else:
                            for index in range(len(formats)):
                                if index + 1 > len(checks):
                                    self._format_list.append(formats[index].strip())
                                    self._check_list.append(False)
                                else:
                                    self._format_list[index] = formats[index].strip()
                                    check_str = checks[index].strip()
                                    self._check_list.append(check_str if check_str != "" else False)
                    else:
                        # print formats
                        for index in range(len(formats)):
                            # print index
                            self._format_list.append(formats[index].strip())
                            self._check_list.append(False)
                    self._value = False
                else:
                    self._format = opt['format'].strip()
                    self._check = opt['check'].strip() if 'check' in opt.keys() else False
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
    def format_list(self):
        """
        多格式支持时返回格式列表
        :return:
        """
        return self._format_list

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
                path_list = value.split("||")
                if len(path_list) > 1:
                    file_path = path_list[1]
                    file_format = path_list[0]
                else:
                    file_path = path_list[0]
                    file_format = None
                if os.path.exists(file_path):
                    # if self.type == "infile":  # 检查输出文件是否满足要求
                        # class_obj = load_class_by_path(self._options[name].format, "File")
                    if file_format is not None:
                        if len(self._format_list) > 1:
                            if file_format not in self._format_list:
                                e = "输入参数%s的文件格式%s必须在范围%s内!" % (self.name, file_format, self._format_list)
                                self._file_check_error(e)
                            else:
                                self._format = file_format
                                self._check = self._check_list[self._format_list.index(file_format)]
                        else:
                            if file_format != self._format:
                                e = "输入参数%s的文件格式必须为%s,不能为%s!" % (self.name, self._format, file_format)
                                self._file_check_error(e)
                        file_obj = self._check_file(self._format, self._check, file_path)
                        if file_obj:
                            self._value = file_obj
                        else:
                            e = "输入参数%s的文件格式不正确!" % self.name
                            self._file_check_error(e)
                    else:
                        if len(self._format_list) > 1:
                            has_pass = False
                            for index in range(len(self._format_list)):
                                format_path = self._format_list[index]
                                check = self._check_list[index]
                                file_obj = self._check_file(format_path, check, file_path, loop=True)
                                if file_obj:
                                    has_pass = True
                                    self._value = file_obj
                                    self._format = format_path
                                    self._check = check
                                    break
                            if not has_pass:
                                e = "输入参数%s的文件格式不正确!" % self.name
                                self._file_check_error(e)
                        else:
                            file_obj = self._check_file(self._format, self._check, file_path)
                            if file_obj:
                                self._value = file_obj
                            else:
                                e = "输入参数%s的文件格式不正确!" % self.name
                                self._file_check_error(e)
                else:
                    raise OptionError("文件不存在！")
            else:
                if len(self._format_list) > 1:
                    has_pass = False
                    self._check_type(value)
                    for index in range(len(self._format_list)):
                        class_obj = load_class_by_path(self._format_list[index], "File")
                        if isinstance(value, class_obj):
                            self._value = value
                            self._format = self._format_list[index]
                            self._check = self._check_list[index]
                            has_pass = True
                            break
                    if not has_pass:
                        e = "参数%s的文件对象类型设置不正确!" % self.name
                        self._file_check_error(e)
                else:
                    self._check_type(value)
                    class_obj = load_class_by_path(self._format, "File")
                    if isinstance(value, class_obj):
                        self._value = value
                    else:
                        e = "参数%s的文件对象类型设置不正确!" % self.name
                        self._file_check_error(e)
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

    def _check_file(self, format_path, check, path, loop=False):
        """

        :param format_path:
        :param check:
        :return:
        """
        class_name = self.bind_obj.__class__.__name__
        self_class_path = get_classpath_by_object(self.bind_obj)
        paths = self_class_path.split(".")[2:]
        function_name = "_".join(paths)
        if re.search(r"Agent$", class_name) or re.search(r"Tool$", class_name):
            function_name += "_tool_check"
        elif re.search(r"Module$", class_name):
            function_name += "_module_check"
        elif re.search(r"Workflow$", class_name):
            function_name += "_workflow_check"
        else:
            raise Exception("类名称不正确!")
        try:
            file_obj = load_class_by_path(format_path, "File")()
            file_obj.set_path(path)
            if check:
                if hasattr(file_obj, check):
                    getattr(file_obj, check)()
                else:
                    raise Exception("文件类%s中未定义指定的检测函数%s!" %
                                    (format_path, check))
            else:
                if hasattr(file_obj, function_name):
                    getattr(file_obj, function_name)()
                else:
                    getattr(file_obj, "check")()
        except FileError:
            exstr = traceback.format_exc()
            if loop:
                self.bind_obj.logger.debug("检测未通过(以下为调试信息，可忽略):\n%s" % exstr)
            else:
                print exstr
            return False
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            self._file_check_error(str(e))
        else:
            return file_obj

    def _file_check_error(self, error=""):
        """
        文件检测错误后的处理

        :param error:
        :return:
        """
        class_name = self.bind_obj.__class__.__name__
        if re.search(r"Tool$", class_name):
            self.bind_obj.set_error(error)
        else:
            self.bind_obj.get_workflow().exit(exitcode=1, data=error)
