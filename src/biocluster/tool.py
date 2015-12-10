# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from .core.actor import RemoteActor
from .core.actor import State
import types
from .command import Command
import gevent
from .logger import Wlog
import sys
import pickle
import os
import threading
import time
import re
import importlib


class Tool(object):
    """
    远程运行工具，与Agent对应
    """

    def __init__(self, config):
        """
        初始化并加载config

        :param config: PickleConfig对象
        :return:
        """
        super(Tool, self).__init__()
        self.config = config
        self._name = ""
        self._full_name = ""
        self._id = ""
        self._work_dir = ""
        self._commands = {}
        self._states = []
        self._output_path = ""
        self._run = False
        self._end = False
        self._options = {}
        self.load_config()
        self.logger = Wlog(self).get_logger('')
        self.actor = RemoteActor(self, threading.current_thread())
        self.mutex = threading.Lock()
        self.exit_signal = False

    @property
    def name(self):
        """
        工具名称，与其Agent名称一致
        :return:
        """
        return self._name

    @property
    def is_end(self):
        """
        是否运行完成
        :return:
        """
        return self._end

    @property
    def id(self):
        """
        id，与其Agent id一致
        :return:
        """
        return self._id

    @property
    def states(self):
        """
        返回发送到远程Agent的状态列表,list State对象列表
        :return:
        """
        return self._states

    @property
    def work_dir(self):
        """
        工作目录

        :return:
        """
        return self._work_dir

    @property
    def output_dir(self):
        return self._output_path

    @property
    def commands(self):
        return self._commands

    def add_command(self, name, cmd):
        """
        执行命令生成Command对象，
        可以在自类中定义命令检测函数.，函数名为 commandname_check ,此类函数将在self.run调用后自动加入微线程运行,3秒钟执行一次
        commandname_check传入参数为名称为commandname的Command对象

        :param cmd: 需要执行的命令，路径必须为相对于配置文件main.conf的software_dir的相对路径
        :return: 返回Command对象
        """
        if name in self._commands.keys():
            raise Exception("命令名称已经存在，请勿重复添加")
        if not isinstance(name, types.StringType):
            raise Exception("命令名称必须为字符串")
        elif not name.islower():
            raise Exception("命令名称必须都为小写字母！")
        else:
            cmd = Command(name, cmd, self)
            self._commands[name] = cmd
            # gevent.spawn(self._resource_record, cmd)
            return cmd

    def get_option_object(self, name=None):
        """
        通过参数名获取当前对象的参数 :py:class:`biocluster.option.Option` 对象

        :param name: string 参数名，可以不输入，当不输出时返回当前对象的所有参数对象 list输出
        :return: :py:class:`biocluster.option.Option` object 或 :py:class:`biocluster.option.Option` object数组
        """
        if not name:
            return self._options
        elif name not in self._options.keys():
            raise Exception("参数%s不存在，请先添加参数" % name)
        else:
            return self._options[name]

    def option(self, name, value=None):
        """
        获取/设置对象的参数值

        :param name: 参数名称
        :param value: 当value==None时，获取参数值 当value!=None是，设置对应的参数值
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

        :param options: dict key是参数名,value是参数值
        :return:
        """
        if not isinstance(options, dict):
            raise Exception("参数格式错误!")
        for name, value in options.items():
            self.option(name, value)

    def wait(self, *cmd_name_or_objects):
        """
        暂停当前线程并等待,指定的Command对象执行完成,如果cmdname未指定，则等待所有Command运行完成

        :cmdname:  一个或多个cmd名称 或Commnad对象
        """
        cmds = []
        if len(cmd_name_or_objects) > 0:
            for c in cmd_name_or_objects:
                if isinstance(c, Command):
                    cmds.append(c)
                else:
                    if c not in self._commands.keys():
                        raise Exception("Commnad名称不存在！")
                    cmds.append(self._commands[c])
        else:
            cmds = self._commands.values()
        while True:
            if self.exit_signal:
                self.logger.info("接收到退出信号，终止程序运行!")
                self.exit()
            if len(self._commands) == 0:
                break
            is_running = False
            for command in cmds:
                gevent.sleep(1)
                if command.is_running:
                        is_running = True
                if not command.has_run:
                    gevent.sleep(1)
                    if command.is_running:
                        is_running = True
            if not is_running:
                break

    def add_state(self, name, data=None):
        """
        添加状态,用于发送远程状态，


        :param name: string State名称
        :param data: 需要传递给Agent相关的数据,data必须为python内置的简单数据类型
        :return: self
        """
        if not isinstance(name, types.StringType):
            raise Exception("状态名称必须为字符串")
        elif not name.islower():
            raise Exception("状态名称必须都为小写字母！")
        else:
            self.mutex.acquire()
            self._states.append(State(name, data))
            self.mutex.release()
        return self

    def run(self):
        """
        开始运行,此方法应该在子类中被扩展

        :return:
        """
        if not self.config.DEBUG:
            self.actor.start()
        self._run = True
        self.logger.info("开始运行!")

    def resource_record(self, command):
        """
        记录运行资源

        :return:
        """
        def friendly_size(size):
            gb = 1024*1024*1024
            mb = 1024*1024
            kb = 1024
            if size >= gb:
                return "%sG" % (size/gb)
            elif size >= mb:
                return "%sM" % (size/mb)
            elif size >= kb:
                return "%sM" % (size/kb)
            else:
                return "%s" % size

        filepath = os.path.join(self.work_dir, command.name+"_resource.txt")
        while True:
            if not command.has_run:
                if command.is_error:
                    break
                continue
            if command.is_running:
                processes = command.get_psutil_processes()
                time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                process_num = len(processes)
                if process_num == 1:
                    try:
                        pid = processes[0].pid
                        cmd = " ".join(processes[0].cmdline())
                        cpu_percent = processes[0].cpu_percent()
                        memory_percent = processes[0].memory_percent()
                        memory_info = processes[0].memory_info()
                        memory_rss = friendly_size(memory_info[0])
                        memory_vms = friendly_size(memory_info[1])
                    except:
                        pass
                    else:
                        with open(filepath, "a") as f:
                            f.write("%s\tmain_pid:%s\tcpu_percent:%s\tmemory_percent:%s\tmemory_rss:%s"
                                    "\tmemory_vms:%s\tcmd:%s\n" % (time_now, pid, cpu_percent, memory_percent,
                                                                   memory_rss, memory_vms, cmd))

                if process_num > 1:
                    with open(filepath, "a") as f:
                        pid = processes[0].pid
                        cmd = " ".join(processes[0].cmdline())
                        f.write("%s\tprocess_num:%s\tmain_pid:%s\tcmd:%s\n" % (time_now, process_num, pid, cmd))
                        for p in processes:
                            try:
                                pid = p.pid
                                cmd = " ".join(p.cmdline())
                                cpu_percent = p.cpu_percent()
                                memory_percent = p.memory_percent()
                                memory_info = p.memory_info()
                                memory_rss = friendly_size(memory_info[0])
                                memory_vms = friendly_size(memory_info[1])
                            except:
                                pass
                            else:
                                f.write("\t\t\tpid:%s\tcpu_percent:%s\tmemory_percent:%s\tmemory_rss:%s\t"
                                        "memory_vms:%s\tcmd:%s\n" % (pid, cpu_percent, memory_percent,
                                                                     memory_rss, memory_vms, cmd))
                gevent.sleep(10)
            else:
                break

    def exit_action(self, data):
        """
        处理远程发回的exit action

        :return:
        """
        self.logger.info("接收到action退出指令:%s" % str(data))
        self.exit()

    def end(self):
        """
        设置Tool已经完成，设置完成后,actor将在发送finish状态后终止发送信息

        :return:
        """
        self.save_output()
        self.add_state('finish')
        self.logger.info("程序运行完成")

    def exit(self, status=1):
        """
        不发送任何信号，立即退出程序运行并终止所有命令运行

        :param status: 退出运行时的exitcode
        :return:
        """
        self.exit_signal = True
        for command in self._commands.values():
            command.kill()
        sys.exit(status)

    def save_output(self):
        """
        将输出参数Option对象写入Pickle文件，便于传递给远程Agent

        :return:
        """
        path = os.path.join(self.work_dir, self.name + "_output.pk")
        output = {}
        for name, option in self._options.items():
            if option.type == 'outfile':
                output[name] = self.option(name)
        with open(path, "w") as f:
            pickle.dump(output, f)
        return path

    def load_config(self):
        """
        从Config对象中加载属性

        :return:
        """
        for name in vars(self.config).keys():
            if hasattr(self, name):
                setattr(self, name, getattr(self.config, name))
        for option in self._options.values():
            option.bind_obj = self

    def set_error(self, error_data):
        """
        设置错误状态,设置完成后,actor将在发送error状态后终止发送信息

        :param error_data:
        :return:
        """
        self.add_state('error', error_data)
        self.logger.info("运行出错:%s" % error_data )
        self.exit_signal = True

    @staticmethod
    def set_environ(**kwargs):
        """
        设置环境变量,清除环境变量使用os.unset_environ

        :param kwargs:  一个或多个带名称的参数,参数名为变量名 value为变量值
        """
        for (key, value) in kwargs.items():
            if key not in os.environ.keys():
                os.environ[key] = value
            else:
                os.environ[key] = value + ":" + os.environ[key]

    @staticmethod
    def unset_environ(**varname):
        """
        清除环境变量

        :param varname:  环境变量名
        :return:   返回被删除的环境变量值
        """
        if varname in os.environ.keys():
            return os.environ.pop(varname)

    @staticmethod
    def load_package(path):
        """
        动态加载mbio.packges下自定义模块的类对象

        :param path: 自动以模块path路径
        :return: class对象
        """
        path = re.sub(r'[^a-zA-Z0-9\._]', '', path)
        name = path.split(".").pop()
        l = name.split("_")
        l = [el.capitalize() for el in l]
        class_name = "".join(l)
        imp = importlib.import_module("mbio.packages." + path)
        return getattr(imp, class_name)
