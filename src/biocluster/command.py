# encoding: utf-8

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
        if self._subprocess:
            return True
        else:
            return False

    @property
    def is_running(self):
        """
        返回是否在运行状态
        """
        if not self._subprocess:
            return False
        if self._subprocess.poll() is None:
            return True
        else:
            return False

    @property
    def return_code(self):
        """
        获取命令退出时返回的状态编码
        """
        if not self._subprocess:
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
            if not self._psutil_process:
                self._psutil_process = psutil.Process(self._pid)
                self._all_processes = [self._psutil_process]
            chidrens = self._psutil_process.children(recursive=True)
            chidrens.insert(0, self._psutil_process)
            for child in self._all_processes:   # 删除已完成的进程
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
                                                env=os.environ)
        except Exception, e:
            self.tool.set_error(e)
        else:
            self._pid = self._subprocess.pid
            self._last_run_cmd = self.cmd

        try:
            tmp_file = os.path.join(self.work_dir, self._name + ".o")
            with open(tmp_file, "w") as f:
                starttime = datetime.datetime.now()
                f.write("%s\t运行开始\n" % starttime)
                while self._subprocess.poll() is None:
                    line = self._subprocess.stdout.readline()
                    if line:
                        f.write(line)
                        if hasattr(self.tool, self.name + '_check'):
                            func = getattr(self.tool, self.name + '_check')
                            argspec = inspect.getargspec(func)
                            args = argspec.args
                            if len(args) != 3:
                                Exception(u"状态监测函数参数必须为3个(包括self)!")
                            func(self, line)   # check function(toolself, command, line)  single line
                else:
                    endtime = datetime.datetime.now()
                    use_time = (endtime - starttime).seconds
                    # time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    f.write("%s\t运行结束，运行时长:%ss,exitcode:%s\n" % (endtime, use_time, self.return_code))
        except IOError, e:
            self.tool.set_error(e)
        return self

    def run(self):
        """
        新建线程开始运行

        :return: self
        """
        if self._pid != "":
            raise OSError(u"命令已经运行，不能重复运行!")
        if "|" in self.cmd or ">" in self.cmd or "<" in self.cmd:
            raise Exception(u"不能使用管道符或重定向符!")
        command = self.software_dir + "/" + self.cmd
        args = shlex.split(command)
        if not os.path.isfile(args[0]):
            self.tool.set_error(u"运行的命令文件不存在")
            raise Exception(u"你所运行的命令文件不存在，请确认!")
        thread = threading.Thread(target=self._run)
        thread.start()
        return self

    def rerun(self):
        """
        重运行命令

        :return:
        """
        if self.cmd == self._last_run_cmd:
            raise Exception(u"如需要重复运行命令，需要先通过set_cmd()方法修改命令参数，不能和原命令一样!")
        if self.has_run:
            if self.is_running:
                self.kill()
            self._subprocess = None
            self._pid = ""
        self.run()
        return self

    def kill(self):
        """
        结束命令运行
        :return:
        """
        if self._subprocess and (not self._subprocess.poll()):
            self._subprocess.kill()
