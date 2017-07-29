# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from .core.actor import RemoteActor
from .core.actor import State
import types
from .command import Command
from .logger import Wlog
import sys
import pickle
import os
import threading
import time
import re
import importlib
from .core.function import stop_thread, friendly_size
from .api.database.base import ApiManager
import signal
import inspect
import psutil
import traceback


# class RemoteData(object):
#     def __init__(self, dict_data):
#         self._data = dict_data
#
#     def __getattr__(self, name):
#         """
#         通过下属步骤的名字直接访问下属步骤对象
#
#         :param name:
#         :return:
#         """
#         if name in self._data.keys():
#             return self._data[name]
#         else:
#             raise Exception("不存在名称为%s的远程数据!" % name)


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
        # self._remote_data = {}
        self.version = 0
        self.instant = False  # 本地进程模式
        self.load_config()  
        self.logger = Wlog(self).get_logger('')
        self.main_thread = threading.current_thread()
        self.mutex = threading.Lock()
        self.exit_signal = False
        # self._remote_data_object = RemoteData(self._remote_data)
        self._rerun = False
        self.process_queue = None
        self.shared_callback_action = None
        # self._has_record_commands = []
        self.is_wait = False
        self.receive_exit_signal = False
        self.api = ApiManager(self)
        if self.instant is True:
            self.actor = None
        else:
            self.actor = RemoteActor(self, self.main_thread)
            if self.config.DEBUG is not True:
                self.actor.start()
                self.logger.debug("启动Actor线程!")

    # @property
    # def remote(self):
    #     """
    #     获取远程数据对象
    #     :return:
    #     """
    #     return self._remote_data_object

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

        :param name:  命令名称
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
            cmds.extend(self._commands.values())
        while True:
            time.sleep(1)
            if self.exit_signal:
                self.logger.info("接收到退出信号，终止程序运行!")
                break
            if len(cmds) == 0:
                break
            is_running = False
            for command in cmds:
                if command.has_run:
                    if command.is_end is False:
                        is_running = True
                    else:
                        command.join_thread()
                else:
                    is_running = True
            if is_running is False:
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
            if self.instant:
                action = self._send_local_state(State(name, data))
                if isinstance(action, dict) and 'action' in action.keys():
                    if action['action'] != "none":
                        if hasattr(self, action['action'] + '_action'):
                            func = getattr(self, action['action'] + '_action')
                            argspec = inspect.getargspec(func)
                            args = argspec.args
                            if len(args) == 1:
                                func()
                            elif len(args) == 2:
                                func(action['data'])
                            else:
                                raise Exception("action处理函数参数不能超过2个(包括self)!")
                        else:
                            self.logger.warn("没有为返回action %s设置处理函数!" % action['action'])
            else:
                with self.mutex:
                    self._states.append(State(name, data))
        return self

    def _send_local_state(self, state):
        self.save_report()
        msg = {"id": self.id,
               "state": state.name,
               "data": state.data,
               "version": self.version
               }
        try:
            self.process_queue.put(msg)
        except Exception, e:
            self.logger.debug("error: %s", e)

        # print "Put MSG:%s" % msg
        key = "%s" % self.version
        action = {'action': 'none'}
        if key in self.shared_callback_action.keys():
            action = self.shared_callback_action.pop(key)
        return action

    def run(self):
        """
        开始运行,此方法应该在子类中被扩展

        :return:
        """
        threading.Thread(target=self.check_command, args=(), name='thread-check-command').start()
        self.logger.debug("启动Check Command线程!")
        signal.signal(signal.SIGTERM, self.exit_handler)
        signal.signal(signal.SIGHUP, self.exit_handler)
        self.logger.info("注册信号处理函数!")
        self._run = True
        self.logger.info("开始运行!")

    def exit_handler(self, signum, frame):
        self.receive_exit_signal = True
        if signum == 0:
            self.logger.debug("检测到父进程终止，准备退出!")
        else:
            self.logger.debug("接收到Linux signal %s 信号，终止运行!" % signum)
        if "SLURM_JOB_ID" in os.environ.keys():
            time.sleep(1)
            self.logger.debug("开始检测SLURM STDERR输出...")
            slurm_error_path = os.path.join(self.work_dir, "%s_%s.err" % (self.name, os.environ["SLURM_JOB_ID"]))
            error_msg = ""
            exceeded = False
            with open(slurm_error_path, "r") as f:
                f.seek(0, 2)
                size = os.path.getsize(slurm_error_path)
                point = 2000 if size > 2000 else size
                f.seek(-point, 2)
                lines = f.readlines()
                for line in lines:
                    if re.match(r"^slurmstepd:", line):
                        error_msg += line
                        if re.match(r"exceeded virtual memory limit \(6011508 > 5767168\), being killed", line):
                            exceeded = True
            if exceeded:
                self.logger.info("检测到内存使用超过申请数被系统杀死!")
                self.add_state("memory_limit", error_msg)
            else:
                self.set_error(error_msg)
        else:
            self.set_error("检测到终止信号，但是原因未知！")

    def resource_record(self, command):
        """
        记录运行资源

        :param command: 需要监控的Commnd对象
        :return:
        """
        filepath = os.path.join(self.work_dir, command.name+"_resource.txt")

        if command.is_running:
            resource = command.check_resource()
            time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            if resource:
                with open(filepath, "a") as f:
                    if len(resource) > 0:
                        r = resource.pop(0)
                        f.write("%s\tmain_pid:%s\tcpu_percent:%s\tmemory_rss:%s\tmemory_vms:%s\tcmd:%s\n" %
                                (time_now, r[0], r[2], friendly_size(r[3][0]), friendly_size(r[3][1]), r[1]))
                        if len(resource) > 0:
                            for r in resource:
                                f.write("\t\t\tChild pid:%s\tcpu_percent:%s\tmemory_rss:%s\tmemory_vms:%s\tcmd:%s\n" %
                                        (r[0], r[2], friendly_size(r[3][0]), friendly_size(r[3][1]), r[1]))

    def exit_action(self, data):
        """
        处理远程agent端发回的exit action

        :param data:  退出指令说明
        :return:
        """
        self.logger.info("接收到action退出指令:%s" % str(data))
        self.exit()

    def rerun_action(self, data):
        """
        处理远程agent端发回的rerun指令

        :param data:  退出指令说明
        :return:
        """
        self.logger.info("接收到rerun退出指令:%s" % str(data))
        self._rerun = True
        self.kill_all_commonds()
        os.chdir(self.work_dir)
        script = sys.executable
        args = " ".join(sys.argv)
        self.logger.info("终止主线程运行...")
        stop_thread(self.main_thread)
        self.logger.info("开始重新运行...")
        self.exit_signal = True
        os.system("%s %s" % (script, args))
        self.exit()

    def end(self):
        """
        设置Tool已经完成，设置完成后,actor将在发送finish状态后终止发送信息

        :return:
        """

        self.save_output()
        self.add_state('finish')
        self._end = True
        self.logger.info("Tool程序运行完成")

    def exit(self, status=1):
        """
        不发送任何信号，立即退出程序运行并终止所有命令运行

        :param status: 退出运行时的exitcode
        :return:
        """
        self.kill_all_commonds()
        if not self.is_end and self.main_thread.is_alive():
            self._end = True
            self.exit_signal = True
            # stop_thread(self.main_thread)
            # os._exit(status)
            if status > 0:
                # os.kill(os.getpid(), signal.SIGTERM)
                os.system("kill -9 %s" % os.getpid())
            sys.exit(status)

        # if self.main_thread.is_alive():
        #     stop_thread(self.main_thread)
        self._end = True
        self.exit_signal = True
        self.logger.info("退出运行")
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

    def save_report(self):
        path = os.path.join(self.work_dir, self.name + "_run_report.pk")
        output = []
        for name, command in self.commands.items():
            resource_use = command.get_resource_use()
            cmd_list = {
                "name": name,
                "cmd": command.cmd,
                "start_time": command.start_time,
                "end_time": command.end_time,
                "run_times": command.run_times,
                "main_pid": command.pid,
                "sub_process_num": command.sub_process_num,
                "max_cpu_use": resource_use[0],
                "max_rss": resource_use[2],
                "average_cpu_use": resource_use[1],
                "average_rss": resource_use[3],
                "return_code": command.return_code,
                "max_vms": resource_use[4],
                "average_vms": resource_use[5],
            }
            output.append(cmd_list)
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
        # self.save_report()
        self.add_state('error', error_data)
        self.exit_signal = True
        self.logger.info("运行出错:%s" % error_data)

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

    def kill_all_commonds(self):
        """
        杀死所有正在运行的Commond
        :return:
        """
        for name, command in self._commands.items():
            if command.is_running:
                self.logger.info("终止命令%s运行..." % name)
                command.kill()
                os.system("kill -9 %s" % command.pid)
        main_process = psutil.Process(os.getpid())
        childs = main_process.children(recursive=True)
        for p in childs:
            p.kill()
            os.system("kill -9 %s" % p.pid)

    def check_command(self):
        while not self.is_end:
            try:
                if not self.main_thread.is_alive():
                    break
                if self.is_end or self.exit_signal:
                    break
                for name, cmd in self.commands.items():
                    if cmd.is_running:
                        self.resource_record(cmd)
            except Exception, e:
                exstr = traceback.format_exc()
                print >> sys.stderr, exstr
                print >> sys.stderr, e
                sys.stderr.flush()
            time.sleep(15)
