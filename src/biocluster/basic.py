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
import gevent
import datetime
import json
# import urllib
from biocluster.api.database.base import ApiManager
from .wsheet import Sheet
import random


class Rely(object):
    """
    依赖对象,保存依赖信息
    """
    count = 0
    sem = BoundedSemaphore(1)

    def __init__(self, *rely):
        with Rely.sem:
            Rely.count += 1
            self._name = "reply%s_%s" % (Rely.count, random.randint(100, 1000))
            self._relys = []
            for r in rely:
                if not isinstance(r, Basic):
                    raise Exception("依赖对象不正确!")
                else:
                    self._relys.append(r)
            # self.add_rely(*rely)

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

    # def add_rely(self, *rely):
    #     """
    #     添加依赖对象，对象必须为BasicObject或其子类的实例
    #
    #     :param rely: 一个获多个对象,必须为 Agent Module 的子类
    #     """
    #     for r in rely:
    #         if not isinstance(r, Basic):
    #             raise Exception("依赖对象不正确!")
    #         else:
    #             self._relys.append(r)

    @property
    def satisfy(self):
        """
        返回依赖对象是否全部完成

        :return: bool
        """
        with Rely.sem:
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
    def __init__(self, *args, **kwargs):
        super(Basic, self).__init__()
        if "parent" in kwargs.keys():
            parent = kwargs["parent"]
        elif len(args) > 0:
            parent = args[0]
        else:
            parent = None
        self._parent = parent
        self._rely = {}
        self._children = []
        self._name = self.__get_min_name()
        self.sem = BoundedSemaphore(1)
        self._logger = None
        self._full_name = self.__get_full_name()
        self._id = self.__name_identifier()
        ids = self._id.split(".")
        if self._parent and isinstance(self._parent, Basic):
            self._work_dir = self._parent.work_dir + "/" + ids.pop(-1)
            self._output_path = self._work_dir + "/output"
            self.debug = self.get_workflow().debug
            if not self.debug:
                self.create_work_dir()
        self.__init_events()
        self._options = {}
        self.UPDATE_STATUS_API = None
        self.IMPORT_REPORT_DATA = False
        self.IMPORT_REPORT_AFTER_END = False
        self._main_step = StepMain(self)
        self.stage_id = None    # pipeline模式时设置stage id值
        self._upload_dir_obj = []  # 需要上传的文件夹对象
        self.api = ApiManager(self)
        self._sheet = None

    def create_work_dir(self):
        """
        建立工作目录

        :return:
        """
        if not os.path.exists(self._work_dir):
            os.makedirs(self._work_dir)
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

    @property
    def sheet(self):
        return self._sheet

    @sheet.setter
    def sheet(self, value):
        if not isinstance(value, Sheet):
            raise Exception("sheet值必须为Sheet对象!")
        self._sheet = value

    @property
    def step(self):
        """
        主步骤

        :return:
        """
        return self._main_step

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
            if re.search((b + "$"), class_name):
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
        if self._parent and isinstance(self._parent, Basic):
            with self._parent.sem:
                for c in self._parent.children:
                    if c.name == self.name:
                        count += 1
                identifier = "%s.%s" % (self._parent.id, identifier)
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
            # if not self._children:                # 第一次添加子模块时初始化childend事件
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
            self._logger = Wlog(workflow).get_logger(self._full_name + "(" + self.id + ")")
            return self._logger

    def __init_events(self):
        """
        添加默认触发事件
        """
        self.add_event("start")  # 开始运行
        self.add_event('end')   # 结束事件 对象结束时触发
        self.on('end', self.__event_end)
        self.add_event('error')
        self.on('error', self.__event_error)
        self.add_event('childend', True)  # 子对象事件结束事件
        self.on('childend', self.__event_childend)
        self.add_event('childerror', True)  # 子对象事件错误事件
        self.add_event("childrerun", True)  # 子对象重新运行
        self.on('childrerun', self.__event_childrerun)

    def __event_end(self):
        """
        """
        if self._parent is not None and isinstance(self._parent, Basic):
            self._parent.fire('childend', self)

    def __event_error(self):
        """
        当出现错误事件时(error)时的处理方式
        """
        if self._parent is not None and isinstance(self._parent, Basic):
            self._parent.fire('childerror', self)

    def __check_relys(self):
        """
        检查依赖是否符合条件，如完成则触发对应的依赖事件
        """
        with self.sem:
            if self._rely:
                for name, rl in self._rely.items():
                    if rl.satisfy:
                        event_name = "%s_%s" % (self.id.lower(), name)
                        self._rely.pop(event_name)

                        # if not self.events[event_name].is_start:
                        #     self.events[event_name].stop()
                        #     self.events[event_name].restart()
                        self.fire(event_name, rl)

    def __event_childend(self, child):
        """
        当有子模块完成时触发
        """
        with self.sem:
            if child not in self.children:
                raise Exception("%s不是%s的子对象!" % (child.name, self.name))
            child.stop_listener()
            # with self.sem:
            self.__check_relys()

    def __event_childrerun(self, child):
        """
        当子模块重运行时重启其事件监听

        :param child:
        :return:
        """
        if child not in self.children:
            raise Exception("%s不是%s的子对象!" % (child.name, self.name))
        if not self.is_start:
            return
        if not child.actor.ready():
            child.actor.kill()
        child.rerun()

    def end(self):
        """
        设置对象为结束状态，并触发end事件。
        此函数将会检测输出目录是否为空，并给出debug提示。
        此函数将会检测输出文件路径是否已经设置，如果没有设置则给出debug提示。
        """
        if not os.listdir(self.output_dir):
            self.logger.debug("输出目录%s为空,你确定已经设置了输出目录?" % self.output_dir)
        for option in self._options.values():
            if option.type == 'outfile':
                if not option.value.is_set:
                    self.logger.debug("输出参数%s没有设置输出文件路径,你确定此处不需要设置?" % option.name)
        self.set_end()
        self.fire('end')

    def on_rely(self, rely, func, data=None):
        """
        添加自定义依赖事件，当所有依赖对象完成时次事件被触发。

        :param rely: 当个 :py:class:`biocluster.module.Module` 或  :py:class:`biocluster.agent.Agent` 对象 或其数组
        :param func:  当 rely参数中的所有对象均完成(is_end is True)时，触发此函数
        :param data:  需要传递的参数
        """
        if isinstance(rely, Basic):
            rely.on("end", func, data)
            return
        if not isinstance(rely, list):
            raise Exception("rely参数必须为list数组!")
        if len(rely) < 2:
            raise Exception("rely数组必须至少有2个元素!")
        rely_list = rely
        rely_list.sort()
        for r in rely_list:
            if not isinstance(r, Basic):
                raise Exception("rely参数必须为Basic或其子类的实例对象!")
            if r not in self._children:
                raise Exception("rely模块必须为本对象的子模块!")
        with self.sem:
            for name, r in self._rely.items():
                if r.rely == rely_list:
                    # event_name = "%s_%s" % (self.id.lower(), name)
                    if self.events[name].is_start:
                        raise Exception("rely条件已经被触发，无法再次绑定事件!")
                    else:
                        self.events[name].stop()
                        self.on(name, func, data)
                        self.events[name].restart()
                    return

            rl = Rely(*rely_list)
            event_name = "%s_%s" % (self.id.lower(), rl.name)
            self.add_event(event_name)
            self.on(event_name, func, data)
            if self.is_start:
                self.events[event_name].start()
            self._rely[event_name] = rl
            # print rely_list, rl, rl.name, self.events[rl.name]

    def run(self):
        """
        开始运行
        """
        self.start_listener()
        paused = False
        workflow = self.get_workflow()
        while workflow.pause:
            if not paused:
                self.logger.info("流程处于暂停状态，排队等待恢复运行!")
            paused = True
            workflow.is_wait = True
            gevent.sleep(1)
        self.fire("start")

    def _get_workflow(self, obj):
        if obj.parent:
            return self._get_workflow(obj.parent)
        else:
            return obj

    def get_workflow(self):
        """
        获取当前workflow对象

        :return:  :py:class:`biocluster.workflow.Workflow` 对象
        """
        return self._get_workflow(self)

    def add_upload_dir(self, dir_path):
        """
        添加需要上传的目录

        :param dir_path: 相对或绝对路径
        :return: UploadDir对象
        """
        if not os.path.isdir(dir_path):
            raise Exception("上传路径%s必须目录" % dir_path)
        rel_path = os.path.relpath(dir_path, self.work_dir)
        m = re.match(r"^\.", rel_path)
        if m:
            raise Exception("只能添加当前工作目录的子目录: %s" % dir_path)
        for i in self._upload_dir_obj:
            if i.upload_path == rel_path:
                # 避免不能重复添加目录的问题出现，暂时这样修改， 20161120 by shenghe
                self.logger.info('重复添加了目录：{} ,由于避免框架错误导致重复添加，此处不做处理，只生成LOG信息,请自行区分是框架问题还是编写错误'.format(dir_path))
                return i
                raise Exception("不能重复添加目录%s!" % dir_path)
        up = UploadDir(self)
        up.path = dir_path
        up.upload_path = rel_path
        self._upload_dir_obj.append(up)
        return up

    @property
    def upload_dir(self):
        """
        获取需要上传的文件夹路径
        :return:  list
        """
        # dir_list = []
        # for dir_obj in self._upload_dir_obj:
        #     dir_list.append(dir_obj.path)
        return self._upload_dir_obj

    def get_upload_files(self):
        """
        获取所有上传文件信息
        :return: 数组
        """
        up_file_list = []
        for obj in self._upload_dir_obj:
            up_data = {
                "dir": obj.upload_path,
                "files": obj.file_list
            }
            up_file_list.append(up_data)
        return up_file_list

    def clone_upload_dir_from(self, obj):
        """
        克隆目的对象的UploadDir对象列表

        :param obj: Module, Agent, Workflow
        :return:
        """
        if not isinstance(obj, Basic):
            raise Exception("obj对象类型不正确!!")
        if self._upload_dir_obj:
            raise Exception("已添加上传路径，不能克隆!!")
        self._upload_dir_obj = obj._upload_dir_obj


class Step(object):
    """
    模块步骤定义
    """
    def __init__(self):
        self._name = None
        self._stats = "wait"
        self._has_state_change = False
        self._start_time = datetime.datetime.now()
        self._end_time = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        """
        设置步骤名称

        :param value:
        :return:
        """
        if re.match(r"[\w_\d]{3,20}", value):
            self._name = value
        elif value in ["name", "stats", "start", "finish", "clean_change",
                       "spend_time", "failed", "pause", "terminated"]:
            raise ValueError("步骤名字不能为%s！" % value)
        else:
            raise ValueError("步骤名字必须为数组、字符或下划线，3-20位！")

    @property
    def has_change(self):
        return self._has_state_change

    @property
    def stats(self):
        return self._stats

    def start(self):
        """
        设置步骤为开始状态

        :return:
        """
        self._stats = "start"
        self._has_state_change = True

    def finish(self):
        """
        设置步骤为完成状态

        :return:
        """
        self._stats = "finish"
        self._has_state_change = True
        self._end_time = datetime.datetime.now()

    def clean_change(self):
        """
        清楚更新状态

        :return:
        """
        self._has_state_change = False

    @property
    def spend_time(self):
        """
        开始到结束花费的时间

        :return:
        """
        if self._end_time:
            return (self._end_time - self._start_time).seconds
        else:
            return 0


class StepMain(Step):
    """
    主步骤
    """

    def __init__(self, basc_obj):
        super(StepMain, self).__init__()
        self._steps = {}
        self.bind_obj = basc_obj
        self._error_info = ""
        self._api_data = {}

    def __getattr__(self, name):
        """
        通过下属步骤的名字直接访问下属步骤对象

        :param name:
        :return:
        """
        if name in self._steps.keys():
            return self._steps[name]
        else:
            raise Exception("不存在名称为%s的步骤!" % name)

    def add_api_data(self, name, value):
        """
        添加额外传送到API的数据，每次update执行后清空

        :param name:
        :param value:
        :return:
        """
        if name == "content":
            raise Exception("名称不能为content！")
        if name in self._api_data.keys():
            raise Exception("名称%s已经存在，不能重复添加！" % name)
        self._api_data[name] = str(value)

    def add_steps(self, *names):
        """
        添加下属步骤

        :param names:
        :return:
        """
        for n in names:
            step = Step()
            step.name = n
            self._steps[n] = step

    def failed(self, info=""):
        """
        设置状态为完成

        :param info: 完成信息
        :return:
        """
        self._stats = "failed"
        self._has_state_change = True
        self._end_time = datetime.datetime.now()
        self._error_info = info

    def finish(self):
        """
        设置步骤为完成状态

        :return:
        """
        workflow = self.bind_obj.get_workflow()
        if self.bind_obj is not workflow:
            raise Exception("此方法只能在workflow中调用！")
        super(StepMain, self).finish()

    def pause(self):
        """
        设置状态为暂停

        :return:
        """
        self._stats = "pause"
        self._has_state_change = True

    def terminated(self, info=""):
        """
        设置状态为终止

        :param info:  终止信息
        :return:
        """
        self._stats = "terminated"
        self._has_state_change = True
        self._end_time = datetime.datetime.now()
        self._error_info = info

    @property
    def api_type(self):
        """
        获取API类型

        :return:
        """
        return self.bind_obj.UPDATE_STATUS_API

    def update(self):
        """
        更新状态到API
            *只有当bind_obj的UPDATE_STATUS为True且在self.api_type不为False时才执行更新操作*

        :return:
        """
        if not self.api_type:
            return

        workflow = self.bind_obj.get_workflow()

        if self.has_change:
            json_obj = {"stage": {
                        "task_id": workflow.sheet.id,
                        "stage_id": self.bind_obj.stage_id if self.bind_obj.stage_id else workflow.sheet.stage_id,
                        "created_ts": datetime.datetime.now(),
                        "error": "%s" % str(self._error_info).replace("\'", " ").replace("\"", " "),
                        "status": self.stats,
                        "run_time": self.spend_time}}

            if self.stats == "finish":
                if len(self.bind_obj.upload_dir) > 0:
                    file_list = []
                    dir_list = []
                    for up in self.bind_obj.upload_dir:
                        for ifile in up.file_list:
                            if ifile["type"] == "file":
                                tmp_dict = dict()
                                tmp_dict["path"] = os.path.join(
                                    os.path.join(self.bind_obj.sheet.output, up.upload_path), ifile["path"])
                                tmp_dict["size"] = ifile["size"]
                                tmp_dict["description"] = ifile["description"]
                                tmp_dict["format"] = ifile["format"]
                                file_list.append(tmp_dict)
                            elif ifile["type"] == "dir":
                                tmp_dict = dict()
                                tmp_path = re.sub("\.$", "", ifile["path"])
                                tmp_dict["path"] = os.path.join(
                                    os.path.join(self.bind_obj.sheet.output, up.upload_path), tmp_path)
                                tmp_dict["size"] = ifile["size"]
                                tmp_dict["description"] = ifile["description"]
                                tmp_dict["format"] = ifile["format"]
                                dir_list.append(tmp_dict)
                    json_obj["files"] = file_list
                    json_obj["dirs"] = dir_list
                if "update_info" in self.bind_obj.sheet.options().keys():
                    json_obj["update_info"] = self.bind_obj.sheet.options()

            post_data = {
                "content": json_obj
            }
            for k, v in self._api_data.items():
                post_data[k] = v
            self._api_data.clear()
            data = {
                "task_id": workflow.sheet.id,
                "api": self.api_type,
                "data": post_data
            }
            workflow.send_log(data)

        array = []
        has_change = False
        for step in self._steps.values():
            if step.has_change:
                has_change = True
                state = {
                    "name": step.name,
                    "status": step.stats,
                    "run_time": step.spend_time
                }
                array.append(state)
                step.clean_change()
        if has_change:
            json_obj = {"step": {
                "task_id": workflow.sheet.id,
                "stage_id": workflow.sheet.stage_id,
                "steps": array
            }}
            post_data = {
                "content": json_obj
            }
            for k, v in self._api_data.items():
                post_data[k] = v
            self._api_data.clear()
            data = {
                "task_id": workflow.sheet.id,
                "api": self.api_type,
                "data": post_data
            }
            workflow.send_log(data)


class UploadDir(object):
    """
    需要远程上传的结果信息文件格式和信息
    """
    def __init__(self, parent):
        self._dir_path = ""
        self._file_list = []
        self._parent = parent
        self._regexp_rules = []
        self._relpath_rules = []
        self.upload_path = ""

    @property
    def path(self):
        return self._dir_path

    @path.setter
    def path(self, dir_path):
        """
        设置需要上传的文件夹路径，将上传所有子文件和文件夹
        :param dir_path:
        :return:
        """

        if not os.path.isdir(dir_path):
            raise Exception("%s路径不是有效的文件夹路径" % dir_path)
        else:
            self._dir_path = os.path.abspath(dir_path)
            if not os.listdir(dir_path):
                self._parent.logger.warning("文件夹%s为空，请确认是否已经拷贝？" % dir_path)

    def add_regexp_rules(self, match_rules):
        """
        使用相对于当前添加的上传文件夹的相对路径添加正则匹配规则

        :param match_rules: 必须为一个二维数组, 每个子数组含有3个字符串元素，第一个元素为正则表达式，
        第二个元素为格式path, 第三个元素为文件或文件夹说明
        :return:
        """
        if not isinstance(match_rules, list):
            raise Exception("匹配规则必须为数组!")
        for rule in match_rules:
            self._regexp_rules.append(rule)

    def add_relpath_rules(self, match_rules):
        """
        添加路径匹配，使用相对于当前添加的上传文件夹的相对路径匹配，当前文件夹使用“.”，匹配

        :param match_rules:必须为一个二维数组, 每个子数组含有3个字符串元素，第一个元素为相对路径，
        第二个元素为格式path, 第三个元素为文件或文件夹说明
        :return:
        """
        if not isinstance(match_rules, list):
            raise Exception("匹配规则必须为数组!")
        for rule in match_rules:
            self._relpath_rules.append(rule)

    def match(self):
        """
        根据添加的regexp_rules和relpath_rules匹配所有文件和文件夹，如果regexp_rules和relpath_rules有冲突，
        则relpath_rules生效，正则有冲突，后添加的规则生效

        :return:
        """
        for i in os.walk(self._dir_path):
            self._file_list.append(ResultFile(i[0], self._dir_path, "dir"))
            for file_name in i[2]:
                self._file_list.append(ResultFile(os.path.join(i[0], file_name), self._dir_path, "file"))
        for r_rule in self._regexp_rules:
            # print r_rule
            pattern = re.compile(r_rule[0])
            for sub_file in self._file_list:
                match = pattern.match(sub_file.relpath)
                # print r_rule[0], sub_file.relpath, match
                if match:
                    sub_file.format = r_rule[1]
                    sub_file.description = r_rule[2]
        for r_rule in self._relpath_rules:
            for sub_file in self._file_list:
                # print sub_file.relpath, r_rule[1]
                if os.path.relpath(sub_file.relpath, r_rule[0]) == ".":
                        sub_file.format = r_rule[1]
                        sub_file.description = r_rule[2]

        for sub_file in self._file_list:
            if sub_file.file_type == "file" and sub_file.format == "":
                self._parent.logger.warning("文件%s没有设置格式，确认此文件真的无法确认格式？" % sub_file.full_path)
        return self

    @property
    def file_list(self):
        """
        文件对象列表

        :return: 数组，数组元素为ResultFile对象
        """
        self.match()
        data = []
        path_list = list()
        for i in self._file_list:
            if i.relpath not in path_list:
                data.append({
                    "path": i.relpath,
                    "type": i.file_type,
                    "format": i.format,
                    "description": i.description,
                    "size": i.size
                })
                path_list.append(i.relpath)
        return data


class ResultFile(object):
    """
    保持单个结果文件的信息
    """
    def __init__(self, full_path, base_path, file_type="file"):
        self.file_type = file_type
        self.full_path = full_path
        self.base_path = base_path
        self.relpath = os.path.relpath(full_path, base_path)
        self.format = ""
        self.description = ""

    @property
    def size(self):
        if self.file_type == "file":
            return os.path.getsize(self.full_path)
        else:
            return ""
