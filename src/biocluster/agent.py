# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""Tool远程代理"""

from .basic import Basic
from .core.actor import LocalActor
import pickle
import os
from .config import Config
from .scheduling.job import JobManager
from .core.function import get_classpath_by_object, load_class_by_path
import datetime
import types
import inspect


class PickleConfig(object):
    """
    保存配置信息，用于发送到远程 :py:class:`biocluster.tool.Tool` 对象
    """
    def __init__(self):
        self._name = ""
        self._full_name = ""
        self._id = ""
        self._work_dir = ""
        self.endpoint = ""
        self._options = ""
        self._output_path = ""
        self.version = ""
        self.SOFTWARE_DIR = ""
        self.KEEP_ALIVE_TIME = ""
        self.MAX_KEEP_ALIVE_TIME = ""
        self._remote_data = ""

    def clone(self, agent):
        """
        从agent的属性中克隆同名属性值到自身

        :param agent:   :py:class:`Agent` 对象
        """
        for name in vars(self).keys():
            if hasattr(agent, name):
                setattr(self, name, getattr(agent, name))
            else:
                setattr(self, name, getattr(agent.config, name))

    def save(self, file_handler):
        """
        保存自身到文件对象

        :param file_handler:   文件句柄
        """
        pickle.dump(self, file_handler)


class Agent(Basic):
    """
    提供 :py:class:`biocluster.tool.Tool` 远程代理，通过Agent-Tool对，使远程Tool信息得到相应的处理
    """
    def __init__(self, parent):
        super(Agent, self).__init__(parent)
        self.config = Config()
        self.actor = LocalActor(self)
        self._queue = "default"
        self._host = ""
        self._cpu = 0
        self._memory = ''
        self._default_callback_action = {'action': 'none'}
        self._callback_action = {}
        self._status = "W"                # W 等待 Q 排队 R 运行 E 完成
        self.add_event('keepaliveout')   # 保持连接超时
        self.on('keepaliveout', self._event_keepaliveout)
        self.add_event('waittimeout')    # 等待超时
        self.on('waittimeout', self._event_waittimeout)
        self.add_event('runstart')       # 远端开始运行
        self.on("runstart", self._event_runstart)
        self.on("error", self._agent_event_error)
        self.add_event("recivestate", loop=True)
        self.on("recivestate", self._call_state_callback)  # 收到state状态时调用对应的处理函数
        self.endpoint = self.get_workflow().rpc_server.endpoint
        self.job = None
        self._run_mode = "Auto"      # 运行模式 Auto表示由 main.conf中的 platform参数决定
        self._job_manager = JobManager()
        self._run_time = None
        self._start_run_time = None
        self._end_run_time = None
        self._rerun_time = 0
        self.is_wait = False
        self._remote_data = {}
        self.version = 0  # 重投递运行次数
        self.shared_callback_action = {}  # 进程共享变量，

    def _call_state_callback(self, message):
        """
        将自定义的callback函数转换为事件处理函数

        :param message:   接收到的消息
        :return:
        """
        if hasattr(self, message['state'] + '_callback'):
            func = getattr(self, message['state'] + '_callback')
            argspec = inspect.getargspec(func)
            args = argspec.args
            if len(args) == 1:
                func()
            elif len(args) == 2:
                func(message['data'])
            else:
                raise Exception("状态回调函数参数不能超过2个(包括self)!")
        else:
            self._default_callback(message)

    def _default_callback(self, message):
        """
        消息处理函数不存在时对默认的处理方法

        :param message:   接收到的消息
        :return:
        """
        self.logger.warning(self.name + "没有定义消息对应的处理函数" + message['state'] + "!")

    @property
    def queue(self):
        """
        获取队列名
        """
        return self._queue

    @queue.setter
    def queue(self, queue):

        self._queue = queue

    @property
    def mode(self):
        """
        返回运行模式，默认"Auto"  表示由 main.conf中的 platform参数决定，可以在子类中重写_run_mode属性来定义运行模式
        """
        return self._run_mode

    def add_remote_data(self, name, value):
        """
        添加需要传递到远程Tool的数据

        :param name:
        :param value:
        :return:
        """
        if name in self._remote_data.keys():
            raise Exception("远程数据名称%s已经存在，请勿重复添加" % name)
        if not isinstance(name, types.StringType):
            raise Exception("远程数据名称必须为字符串")
        elif not name.islower():
            raise Exception("命令名称必须都为小写字母！")
        if not (isinstance(value, types.StringTypes) or isinstance(value, types.BooleanType) or
                isinstance(value, types.IntType) or isinstance(value, types.LongType) or
                isinstance(value, types.FloatType) or isinstance(value, types.TupleType) or
                isinstance(value, types.ListType) or isinstance(value, types.DictType)):
            raise Exception("远程数据值必须为Python内置数据类型: 字符串，数字，布尔，list,tuple,dict！")
        self._remote_data[name] = value

    def set_queue(self, queue):
        """
        设置队列名

        :param queue:
        :return: None
        """
        self._queue = queue

    def get_resource(self):
        """
        获取所需资源，必须通过在子类中重写 :py:func:`set_resource` ，并定义 self._cpu 和 self._memory 属性

        :return: cpu,memory
        """
        self.set_resource()
        if self._cpu < 1:
            raise Exception("必须重写方法set_resource,并指定所需资源")
        return self._cpu, self._memory

    def set_resource(self):
        """
        设置所需资源，需在子类中重写此方法 self._cpu ,self._memory

        :return:
        """
        self._cpu = 0
        self._memory = "1GB"

    def save_config(self):
        """
        保存远程 :py:class:`biocluster.tool.Tool` 运行所需参数

        :return: 文件路径
        """
        path = os.path.join(self.work_dir, self.name + ".pk")
        for option in self._options.values():
            option.bind_obj = None
        with open(path, "w") as f:
            config = PickleConfig()
            config.clone(self)
            config.save(f)
        for option in self._options.values():
            option.bind_obj = self
        return path

    def save_class_path(self):
        """
        保存加载运行远程 :py:class:`biocluster.tool.Tool` 所需的类包路径

        :return: path  文件路径
        """
        path = os.path.join(self.work_dir, self.name + "_class.pk")
        class_list = {"tool": get_classpath_by_object(self)}   # 类文件路径
        file_class_paths = []  #
        for option in self._options.values():
            if option.type in {'outfile', 'infile'}:
                if option.format:
                    file_class_paths.append(option.format)
                else:
                    for f in option.format_list:
                        file_class_paths.append(f)
        class_list['files'] = file_class_paths
        with open(path, "w") as f:
            pickle.dump(class_list, f)
        return path

    def load_output(self):
        """
        从远端 :py:class:`biocluster.tool.Tool` 保存的pk文件中读取Option输出值，并赋值给自身Option参数

        :return:
        """
        output_path = os.path.join(self.work_dir, self.name + "_output.pk")
        with open(output_path, "r") as f:
            output = pickle.load(f)
        for option in self._options.values():
            if option.type in {'outfile', 'infile'}:
                if option.format:
                    load_class_by_path(option.format, "File")
                else:
                    for f in option.format_list:
                        load_class_by_path(f, "File")
        for name, value in output.items():
            self.option(name, value)

    def run(self):
        """
        开始运行，并投递任务

        :return:
        """
        super(Agent, self).run()
        if self.get_workflow().sheet.instant:
            self._run_mode = "process"
        else:
            self.save_class_path()
            self.save_config()
        self.job = self._job_manager.add_job(self)
        self._run_time = datetime.datetime.now()
        self._status = "Q"
        if not self.get_workflow().sheet.instant:
            self.actor.start()

    def rerun(self):
        """
        删除远程任务并重新运行

        :return:
        """
        self._rerun_time += 1
        if self._rerun_time > 3:
            self.fire("error", "重运行超过3次仍未成功!")
        else:
            self.stop_listener()
            self.restart_listener()
            self.version += 1
            self._work_dir += "_%s" % self.version
            self._output_path = self._work_dir + "/output"
            self.create_work_dir()
            if not self.get_workflow().sheet.instant:
                self.save_class_path()
                self.save_config()
                self.actor.kill()
            self._run_time = datetime.datetime.now()
            self._status = "Q"
            self.actor.auto_break = True
            # self.actor.kill()
            self.logger.info("开始重新投递任务!")
            self.job.resubmit()
            self.actor = LocalActor(self)
            self.actor.start()

    def _set_callback_action(self, action, data=None, version=None):
        """
        设置需要发送给远程的Action指令,只会被远程获取一次

        :param action: string
        :param data: python内置数据类型,需要被传递给远程的数据
        :param version: agent版本编号
        :return:
        """
        if version is None:
            version = self.version
        if self.get_workflow().sheet.instant:
            self._callback_action = self.shared_callback_action
        self._callback_action["%s" % version] = {'action': action, 'data': data}

    def get_callback_action(self, version):
        """
        获取需要返回给远程的Action信息,此信息只会被获取一次

        :param version: 远程tool版本号
        :return: None
        """
        key = "%s" % version
        if key in self._callback_action.keys():
            action = self._callback_action[key]
            del self._callback_action[key]
        else:
            action = self._default_callback_action
        return action

    def send_exit_action(self, reason="", version=None):
        """
        发送exit指令到远程tool,此指令将导致远程Tool自动退出

        :param reason: 发送exit指令的原因
        :param version: agent版本编号
        :return:
        """
        if version is None:
            version = self.version
        self.logger.info("发送exit指令到远程Tool 版本%s ...." % version)
        self._set_callback_action("exit", data=reason, version=version)

    def send_rerun_action(self, reason, version=None):
        """
        发送rerun指令到远程tool,此指令讲导致远程Tool重新运行。
        一般情况下，需要先修改agent自身的option参数，然后再发送此指令，修改后的参数值将在远程Tool中生效。
        警告：未经修改option而直接发送此指令，将导致远程Tool终止并重新运行，并不会产生其他的效果。

        :param reason: 发送rerun指令的原因
        :param version: agent版本编号
        :return:
        """
        if version is None:
            version = self.version
        self.logger.info("发送rerun指令到远程Tool,版本%s ...." % version)
        self.save_class_path()
        self.save_config()
        self._set_callback_action("rerun", data=reason, version=version)

    def finish_callback(self):
        """
        收到远程发送回的 :py:class:`biocluster.core.actor.State` end状态时的处理函数，设置当前Agent状态为结束

        :return:
        """
        self.load_output()
        self._status = "E"
        self._end_run_time = datetime.datetime.now()
        secends = (self._end_run_time - self._start_run_time).seconds
        self.logger.info("任务运行结束，运行时间:%ss" % secends)
        self.job.set_end()
        self.end()

    def error_callback(self, data):
        """
        收到远程发送回的 :py:class:`biocluster.core.actor.State` error 错误状态时的处理函数, 默认触发error事件，并发送Action指令命令远程Tool退出::

            如不希望发现错误时退出，可以重写此方法

        :param data: 远程state信息中的data信息
        :return: None
        """
        self.fire("error", data)

    def _agent_event_error(self, data):
        """
        Agent发生错误时默认的处理方式

        :return:
        """
        self.logger.error("发现运行错误:%s" % data)
        self.job.set_end()
        self.get_workflow().exit(data="%s %s" % (self.fullname, data))

    def _event_keepaliveout(self):
        """
        当远程 :py:class:`biocluster.tool.Tool` 通信间隔时间超过规定时，执行此方法 在main.conf max_keep_alive_time中配置此时间

        默认情况下将会删除远程Tool所属的任务，并重新投递

        :return:
        """
        self.logger.error("远程Tool连接超时，尝试重新运行!")
        if self.parent:
            self.parent.fire("childrerun", self)

    def _event_waittimeout(self):
        """

        当远程 :py:class:`biocluster.tool.Tool` 超过规定时间未能启动通信时执行此方法，在main.conf max_wait_time中配置此时间

        默认情况下将会删除远程Tool所属的任务，并重新投递

        :return:
        """
        self.logger.error("远程任务超过规定时间未能运行，尝试删除任务重新运行!")
        if self.parent:
            self.parent.fire("childrerun", self)

    def _event_runstart(self, data):
        """

        :param data:
        :return:
        """
        self._start_run_time = datetime.datetime.now()
        self._status = "R"
        secends = (self._start_run_time - self._run_time).seconds
        self.logger.info("远程任务开始运行，任务ID:%s,远程主机:%s,:排队时间%ss" % (self.job.id, data, secends))
