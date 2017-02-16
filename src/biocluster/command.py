# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""
任务命令监控
"""

import shlex
import subprocess
import os
import psutil
import threading
import datetime
import inspect
import copy


class Command(object):
    """
    命令模块，在 :py:class:`biocluster.agent.Tool` 中调用add_commond方法将生成此类的一个对象.
    Command对象负责运行一个命令，并监控其运行过程

    :param name:  string 名称
    :param cmd:  需要执行的命令，可执行文件路径必须为相对于配置文件main.conf的software_dir的相对路径
    :param tool: 调用此对象的 :py:class:`biocluster.agent.Tool` 实例
    """
    def __init__(self, name, cmd, tool):
        """
        初始化生成Command对象,将会新建一个线程负责运行cmd
        """
        super(Command, self).__init__()
        # Command.count += 1
        self._pid = ""
        self.cmd = cmd
        self._last_run_cmd = None
        self.tool = tool
        self.config = tool.config
        self.work_dir = tool.work_dir
        self.software_dir = self.config.SOFTWARE_DIR.rstrip("/")
        self._name = name
        self._subprocess = None
        self.threading_lock = threading.Lock()
        self._psutil_process = None
        self._all_processes = []
        self._is_error = False
        # self.process_status = None
        # self.check_sleep = check_sleep
        # self.max_sleep_time = max_sleep_time
        # self.max_run_times = max_run_times
        self._run_times = 0
        # self.to_rerun = False

    @property
    def is_error(self):
        return self._is_error

    @property
    def pid(self):
        """
        命令运行的系统pid
        :return:
        """
        return self._pid

    @property
    def name(self):
        """
        命令名称
        :return:
        """
        return self._name

    @property
    def has_run(self):
        """
        返回是否已经开始运行
        """
        if self._subprocess is not None or self.is_error:
            return True
        else:
            return False

    @property
    def is_running(self):
        """
        返回是否在运行状态
        """
        if not self._subprocess or self.is_error:
            return False
        try:
            if self._subprocess.poll() is None:
                return True
            else:
                return False
        except Exception, e:
            print e
            return False

    @property
    def return_code(self):
        """
        获取命令退出时返回的状态编码
        """
        if not self._subprocess or self.is_error:
            return None
        return self._subprocess.returncode

    def set_cmd(self, cmd):
        """
        重设需要运行的命令

        :param cmd:  命令文本，可执行文件路径必须为相对于配置文件main.conf的software_dir的相对路径
        :return:
        """
        self.cmd = cmd

    def get_psutil_processes(self):
        """
        返回当前命令及其子进程psutil.Process对象

        :return: list of all child psutil_process objects
        """
        if self.is_running:
            try:
                if not self._psutil_process:
                    self._psutil_process = psutil.Process(self._pid)
                    self._all_processes = [self._psutil_process]
                chidrens = self._psutil_process.children(recursive=True)
                chidrens.insert(0, self._psutil_process)
                all_process = copy.copy(self._all_processes)
                for child in all_process:   # 删除已完成的进程
                    found = False
                    for p in chidrens:
                        if p.pid == child.pid:
                            found = True
                    if not found:
                        self._all_processes.remove(child)
                for p in chidrens:                # 添加新进程
                    found = False
                    for child in self._all_processes:
                        if p.pid == child.pid:
                            found = True
                    if not found:
                        self._all_processes.append(p)
            except Exception, e:
                self.tool.logger.debug("获取命令%s进程时发生错误: %s" % (self.name, e))
        return self._all_processes

    def _run(self):
        """
        运行命令

        :return: self
        """
        command = self.software_dir + "/" + self.cmd
        args = shlex.split(command)
        try:
            self._subprocess = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                env=os.environ, universal_newlines=True)
        except Exception, e:
            self._is_error = True
            self.tool.set_error(e)
        else:
            self._pid = self._subprocess.pid
            self._last_run_cmd = self.cmd
        if not self._subprocess:
            return self
        try:
            count = 0
            tmp_file = os.path.join(self.work_dir, self._name + ".o")
            with open(tmp_file, "w") as f:
                starttime = datetime.datetime.now()
                f.write("%s\t运行开始\n" % starttime)
                f.flush()
                func = None
                if hasattr(self.tool, self.name + '_check'):
                    func = getattr(self.tool, self.name + '_check')
                    argspec = inspect.getargspec(func)
                    args = argspec.args
                    if len(args) != 3:
                        raise Exception("状态监测函数参数必须为3个(包括self)!")
                while True:
                    if self.tool.is_end or self.tool.exit_signal:
                        break
                    if self.is_error or not self.is_running:
                        break
                    line = self._subprocess.stdout.readline()
                    if not line:
                        if not self.is_running:
                            break
                        else:
                            continue
                    if count < 5000:
                        f.write(line)
                    if count == 5000:
                        f.write("输出过大，后续省略...\n")
                    f.flush()
                    count += 1
                    if func is not None:
                        line = line.strip()
                        func(self, line)   # check function(toolself, command, line)  single line
                endtime = datetime.datetime.now()
                use_time = (endtime - starttime).seconds
                # time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                f.write("%s\n运行结束，运行时长:%ss,exitcode:%s\n" % (endtime, use_time, self.return_code))
        except IOError, e:
            self.tool.set_error("运行命令%s出错: %s" % (self.name, e))
        return self

    def run(self):
        """
        新建线程开始运行

        :return: self
        """
        command = self.software_dir + "/" + self.cmd
        self.tool.logger.info("命令内容为{}".format(command))
        if self._pid != "":
            raise OSError("命令已经运行，不能重复运行!")
        if "|" in self.cmd or ">" in self.cmd or "<" in self.cmd:
            raise Exception("不能使用管道符或重定向符!")
        args = shlex.split(command)
        if not os.path.isfile(args[0]):
            self.tool.set_error("运行的命令文件不存在")
            raise Exception("你所运行的命令文件不存在，请确认!")
        thread = threading.Thread(target=self._run)
        thread.start()
        self._run_times += 1
        return self

    def rerun(self):
        """
        重运行命令

        :return:
        """
        if self.cmd == self._last_run_cmd:
            if self._run_times > 3:
                raise Exception("重复运行相同的命令不能超过3次！命令:%s" % self.cmd)
            self.tool.logger.info('重新运行了相同的命令')  # shenghe modified 20161215

        else:
            self._run_times = 0

        if self.is_running:
            self.kill()
        self._subprocess = None
        self._pid = ""
        self._is_error = False
        self.run()
        return self

    def kill(self):
        """
        结束命令运行
        :return:
        """
        if self.is_running:
            chidrens = psutil.Process(self.pid).children(recursive=True)
            for p in chidrens:
                p.kill()
            self._subprocess.kill()
            self._is_error = True
