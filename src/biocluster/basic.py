# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""workflow,module,tool的基础抽象类"""

from .core.event import EventObject
import re
from .logger import Wlog
from .option import Option
import os
from .core.exceptions import OptionError
from gevent.lock import BoundedSemaphore


class Rely(object):
    """
    依赖对象,保存依赖信息
    """
    count = 0
    sem = BoundedSemaphore(1)

    def __init__(self, *rely):
        with Rely.sem:
            Rely.count += 1
            self._name = "reply" + str(Rely.count)
            self._relys = []
            self.add_rely(*rely)

    @property
    def name(self):
        """
        名称
        """
        return self._name

    @property
    def rely(self):
        """
        依赖对象 list列表
        """
        return self._relys

    def add_rely(self, *rely):
        """
        添加依赖对象，对象必须为BasicObject或其子类的实例

        :param rely: 一个获多个对象,必须为 Agent Module 的子类
        """
        for r in rely:
            if not isinstance(r, Basic):
                raise Exception("依赖对象不正确!")
            else:
                self._relys.append(r)

    @property
    def satisfy(self):
        """
        返回依赖对象是否全部完成

        :return: bool
        """
        is_end = True
        if self._relys:
            for r in self._relys:
                if not r.is_end:
                    is_end = False
        else:
            raise Exception("依赖对象不能为空!")
        return is_end


class Basic(EventObject):
    """
    基础抽象类，定义了 :py:class:`biocluster.workflow.Workflow` , :py:class:`biocluster.module.Module` ,
    :py:class:`biocluster.agent.Agent` 的公共方法和属性
    """
    def __init__(self, parent=None):
        super(Basic, self).__init__()
        self._parent = parent
        self._rely = []
        self._children = []
        self._name = self.__get_min_name()
        self._full_name = self.__get_full_name()
        self._id = self.__name_identifier()
        self.__init_events()
        self._logger = None
        if parent:
            ids = self._id.split(".")
            self._work_dir = parent.work_dir + "/" + ids.pop(-1)
            if not os.path.exists(self._work_dir):
                os.makedirs(self._work_dir)
            self._output_path = self._work_dir + "/output"
            if not os.path.exists(self._output_path):
                os.makedirs(self._output_path)
        self._options = {}
        self.sem = BoundedSemaphore(1)
        self.API_TYPE = None

    @property
    def name(self):
        """
        名称，自动获取类名称的前半部分，如::

            TestAgent 名称为 Test  AbcDefWorkflow名称为 AbcDef
        """
        return self._name

    @property
    def id(self):
        """
        id值,当一个 :py:class:`biocluster.workflow.Workflow` 或 :py:class:`biocluster.module.Module`
        拥有多个同名的 :py:attr:`children` 时，或自动按照先后为其编号,并在前加上 :py:attr:`parent` 的id
        如: W001.Mtest1.Blast
        """
        return self._id

    @property
    def fullname(self):
        """
        全名，在 :py:attr:`name` 前加上 :py:attr:`parent` 的 :py:attr:`fullname`
        """
        return self._full_name

    @property
    def children(self):
        """
        返回所有下级对象,
        :py:class:`biocluster.workflow.Workflow` 可以添加   :py:class:`biocluster.module.Module` 和
        :py:class:`biocluster.agent.Agent` 下级。

        :py:class:`biocluster.module.Module`  可以添加  :py:class:`biocluster.agent.Agent` 下级

        :return:
        """
        return self._children

    @property
    def parent(self):
        """
        返回当前对象的上级,相对于 :py:attr:`parent`
        """
        return self._parent

    @property
    def work_dir(self):
        """
        返回当前对象的工作目录,路径为上级工作目录加上当前工作对象的id（不加上上级名称）
        :return:
        """
        return self._work_dir

    @property
    def output_dir(self):
        """
        返回当前对象的输出结果路径，扩展子类时需要将最终结果放置到此目录下，不能存放除输出结果外的任何文件。
        默认路径为当前对象  :py:attr:`work_dir` 下的 output目录。

        也可以直接对本属性进行复制，但必须是一个已经存在的目录，否则或报错，如:

            self.output_dir = child.output_dir

        """
        return self._output_path

    @output_dir.setter
    def output_dir(self, value):
        if not os.path.isdir(value):
            raise Exception("目录%s不存在，请确认!" % value)
        else:
            self._output_path = value

    def get_option_object(self, name=None):
        """
        通过参数名获取当前对象的参数 :py:class:`biocluster.option.Option` 对象

        :param name: string 参数名，可以不输入，当不输出时返回当前对象的所有参数对象 list输出
        :return: :py:class:`biocluster.option.Option` object 或 :py:class:`biocluster.option.Option` object数组
        """
        if not name:
            return self._options
        elif name not in self._options.keys():
            raise OptionError("参数%s不存在，请先添加参数" % name)
        else:
            return self._options[name]

    def option(self, name, value=None):
        """
        获取/设置对象的参数值

        :param name: 参数名称
        :param value: 当value==None时，获取参数值 当value!=None时，设置对应的参数值
        :return: 参数对应的值
        """
        if name not in self._options.keys():
            raise Exception("参数%s不存在，请先添加参数" % name)
        if value is None:
            return self._options[name].value
        else:
            self._options[name].value = value

    def set_options(self, options):
        """
        批量设置参数值

        :param options: 字典,其中key是参数名,value是参数值
        :return: None
        """
        if not isinstance(options, dict):
            raise Exception("参数格式错误!")
        for name, value in options.items():
            self.option(name, value)
        self.check_options()

    def check_options(self):
        """
        检测option值是否满足运行需求,对于不满足要求的，应该抛出OptionError异常，满足要求的，返回True

        调用set_options方法时会自动执行此函数，需要在子类中予以重新

        如不通过set_options方法设置参数，应该手动调用此函数

        :return: True or False
        """
        pass

    def add_option(self, option):
        """
        解析定义的参数设置，并生成 :py:class:`biocluster.option.Option` 设置为参数

        :param option: list

        格式说明::

                options = [
                    {"name": "customer_mode", "type": "bool", "default": False},  # customer 自定义数据库
                    {"name": "query", "type": "infile", "format": "fasta"},  # 输入文件
                    {"name": "database", "type": "string", "default": "nr"},
                                            # 比对数据库 nt nr strings GO swissprot uniprot KEGG
                    {"name": "reference", "type": "infile", "format": "fasta"},  # 参考序列  选择customer时启用
                    {"name": "evalue", "type": "float", "default": 1e-5},  # evalue值
                    {"name": "num_threads", "type": "int", "default": 10},  # cpu数
                    {"name": "output", "type": "outfile", "format": "fasta"}  # cpu数
                ]

                name :参数名
                type :参数类型，参数类型有: int(整数) float(浮点数)  bool(是否选择)  string(字符串或unicode字符串) infile(输入文件) outfile(输出文件)
                default: 参数默认值，一般情况下除输入输出文件外都应该有默认值
                format: 文件格式,应该使用文件类对于的动态加载path （请参考教程中相应的内容）,所有输出输出文件都应该有其对应的format

        :return: None
        """
        if not isinstance(option, list):
            raise Exception("参数格式错误!")
        for opt in option:
            if not isinstance(opt, dict) or 'name' not in opt.keys():
                raise Exception("参数格式错误!")
            self._options[opt['name']] = Option(opt, bind_obj=self)

    def __get_min_name(self):
        """
        获取模块的简写名称,去除后缀'Module','Tool', "Workflow","Agent"
        """
        class_name = self.__class__.__name__
        base = ['Basic', 'Module', 'Tool', 'Agent', 'Workflow']

        if class_name in base:
            raise Exception("抽象类%s不允许实例化!" % class_name)
        for b in base:
            if re.search((b+"$"), class_name):
                # return re.sub((b + "$"), '', class_name).lower()
                return re.sub((b + "$"), '', class_name)
        return class_name

    def __get_full_name(self):
        """
        获取完整路径名,如Module.Tool
        """
        name = self._name
        if self._parent and self._parent.fullname != "":
            name = self._parent.fullname + "." + name
        return name

    def __name_identifier(self):
        """
        对于同一模块的子模块中多个同类型模块编号
        """
        identifier = self._name
        count = 0
        if self._parent:
            with self._parent.sem:
                for c in self._parent.children:
                    if c.name == self.name:
                        count += 1
                identifier = self._parent.id + "." + identifier
        if count > 0:
            identifier += str(count)
        return identifier

    def add_child(self, *child):
        """
        添加子模块，:py:attr:`children` 中增加一个子模块实例

        :param child: 一个或多个 :py:class:`biocluster.module.Module` 或  :py:class:`biocluster.agent.Agent` 对象
        :return: self
        """
        for c in child:
            if not isinstance(c, Basic):
                raise Exception("child参数必须为Basic或其子类的实例对象!")
            if not self._children:                # 第一次添加子模块时初始化childend事件
                self.add_event('childend', True)  # 子对象事件结束事件
                self.on('childend', self.__event_childend)
                self.add_event('childerror', True)  # 子对象事件错误事件
                self.add_event("childrerun", True)  # 子对象重新运行
                self.on('childrerun', self.__event_childrerun)
            self._children.append(c)
        return self

    @property
    def logger(self):
        """
        返回 :py:class:`biocluster.logger.Wlog` 对象
        """
        if self._logger:
            return self._logger
        else:
            workflow = self.get_workflow()
            self._logger = Wlog(workflow).get_logger(self._full_name + "(" + workflow.id + ")")
            return self._logger

    def __init_events(self):
        """
        添加默认触发事件
        """
        self.add_event('end')   # 结束事件 对象结束时触发
        self.on('end', self.__event_end)
        self.add_event('error')
        self.on('error', self.__event_error)

    def __event_end(self):
        """
        """
        if self._parent:
            self._parent.fire('childend', self)

    def __event_error(self):
        """
        当出现错误事件时(error)时的处理方式
        """
        if self._parent:
            self._parent.fire('childerror', self)

    def __check_relys(self):
        """
        检查依赖是否符合条件，如完成则触发对应的依赖事件
        """
        if self._rely:
            for rl in self._rely:
                if rl.satisfy:
                    self.fire(rl.name, rl)

    def __event_childend(self, child):
        """
        当有子模块完成时触发
        """
        child.stop_listener()
        self.__check_relys()

    def __event_childrerun(self, child):
        """
        当子模块重运行时重启其事件监听

        :param child:
        :return:
        """
        if self.is_start:
            child.stop_listener()
            child.restart_listener()

    def end(self):
        """
        设置对象为结束状态，并触发end事件。
        此函数将会检测输出目录是否为空，并给出debug提示。
        此函数将会检测输出文件路径是否已经设置，如果没有设置则给出debug提示。
        """
        self.set_end()
        self.fire('end')
        if not os.listdir(self.output_dir):
            self.logger.debug("输出目录%s为空,你确定已经设置了输出目录?" % self.output_dir)
        for option in self._options.values():
            if option.type == 'outfile':
                if not option.value.is_set:
                    self.logger.debug("输出参数%s没有设置输出文件路径,你确定此处不需要设置?" % option.name)

    def on_rely(self, rely, func,  data=None):
        """
        添加自定义依赖事件，当所有依赖对象完成时次事件被触发。

        :param rely: 当个 :py:class:`biocluster.module.Module` 或  :py:class:`biocluster.agent.Agent` 对象 或其数组
        :param func:  当 rely参数中的所有对象均完成(is_end is True)时，触发此函数
        :param data:  需要传递的参数
        """

        if not isinstance(rely, list):
            rely_list = [rely]
        else:
            rely_list = rely
        for r in rely_list:
            if not isinstance(r, Basic):
                raise Exception("rely参数必须为Basic或其子类的实例对象!")
            if r not in self._children:
                raise Exception("rely模块必须为本对象的子模块!")
        with self.sem:
            rl = Rely(*rely_list)
            self._rely.append(rl)
            self.add_event(rl.name)
            self.on(rl.name, func, data)
            if self.is_start:
                self.events[rl.name].start()
            # print rely_list, rl, rl.name, self.events[rl.name]

    def run(self):
        """
        开始运行
        """
        self.start_listener()

    def get_workflow(self):
        """
        获取当前workflow对象

        :return:  :py:class:`biocluster.workflow.Workflow` 对象
        """
        obj = self
        if self.parent:
            obj = self.parent
        if obj.parent:
            obj = obj.parent
        return obj
