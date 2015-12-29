# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""actor消息处理机制"""

import gevent
from gevent import Greenlet
import datetime
import threading
import inspect
import zerorpc
import sys
import platform
import traceback
import os


class State(object):
    def __init__(self, name, data=None):
        self.name = name
        self.data = data


class LocalActor(gevent.Greenlet):
    """
    继承至Greenlet,每个Agent对象都会生成一个LocalActor对象，循环处理接受的消息并负责记录最新状态更新时间,根据接收到的不同信息调用不同的处理函数,如果处理函数不存在则调用默认处理函数予以提示
    """
    def __init__(self, agent):
        # self.inbox = Queue()
        self._config = agent.config
        self._agent = agent
        # self._name = agent.name + ".Actor"
        self._update = None    # 最近接受消息时间
        self._start_time = None  # 开始运行时间
        self._keep_alive_out_fired = False
        self._wait_time_out_fired = False
        Greenlet.__init__(self)

    def check_time(self):
        now = datetime.datetime.now()
        if not self._keep_alive_out_fired:
            if self._update is not None:
                if (now - self._update).seconds > self._config.MAX_KEEP_ALIVE_TIME:
                    self._agent.fire('keepaliveout')
                    self._keep_alive_out_fired = True
        if not self._wait_time_out_fired and self._update is None:
            if not self._agent.is_wait:
                if self._start_time is not None:
                    if (now - self._start_time).seconds > self._config.MAX_WAIT_TIME:
                        self._agent.fire('waittimeout')
                        self._wait_time_out_fired = True

    def receive(self, message):
        """
        接收消息后解析消息数据，动态调用对应的处理方法

        :param message: message为远程rpc传递的数据,python dict类型数据 必须包含 key "event"
        """
        if self._agent.is_end:
            self._agent.logger.debug("已经停止运行，丢弃接收到的消息: %s " % message)
            return
        try:
            if self._update is None:
                self._agent.fire('runstart', message["data"])
            self._update = datetime.datetime.now()
            if (not isinstance(message, dict)) or ('state' not in message.keys()):
                self._agent.logger.warning("接收到不符合规范的消息，丢弃!")
            if message['state'] != "keepalive":
                if hasattr(self._agent, message['state']+'_callback'):
                    func = getattr(self._agent, message['state']+'_callback')
                    argspec = inspect.getargspec(func)
                    args = argspec.args
                    if len(args) == 1:
                        func()
                    elif len(args) == 2:
                        func(message['data'])
                    else:
                        raise Exception("状态回调函数参数不能超过2个(包括self)!")
                else:
                    self.default_callback(message)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            self._agent.get_workflow().exit(exitcode=1, data=e)

    def default_callback(self, message):
        """
        消息处理函数不存在时对默认的处理方法
        """
        self._agent.logger.warning(self._agent.name + "没有定义消息对应的处理函数" + message['state'] + "!")

    def _run(self):
        self._start_time = datetime.datetime.now()
        self.running = True
        while self.running:
            # message = self.inbox.get()
            # self.receive(message)
            self.check_time()
            if self._agent.is_end:
                break
            gevent.sleep(3)


class RemoteActor(threading.Thread):
    """
    每个Tool远程运行时都会产生一个RemoteActor对象,负责远端运行时的消息处理机制。
    负责将Tool运行过程中添加的State状态发送到其对应的Agent对象，并调用对应的函数进行处理。如果Agent返回了Action命令，则调用对于的Action方法
    """
    def __init__(self, tool, main_thread):
        super(RemoteActor, self).__init__()
        self._tool = tool
        self.config = tool.config
        self.mutex = threading.Lock()
        self._lost_connection_count = 0
        self.main_thread = main_thread
        self._has_record_commands = []

    def run(self):
        """
        定时检测Tool的State状态，并发送状态到远端

        :return: None
        """
        gevent.spawn(self.check_command)
        while (not self._tool.is_end) or len(self._tool.states) > 0:
            if self._tool.exit_signal and len(self._tool.states) == 0:
                self._tool.logger.debug("接收到退出信号，终止Actor信号发送!")
                break
            # is_main_thread_active = True
            # for i in threading.enumerate():
            #     if i.name == "MainThread":
            #         is_main_thread_active = i.is_alive()
            if not self.main_thread.is_alive() and len(self._tool.states) == 0 and self._tool.exit_signal is not True:
                self.send_state(State('error', "检测到远程主线程异常结束"))
                self._tool.logger.debug("检测到主线程已退出，终止运行!")
                break
            if len(self._tool.states) > 0:
                self.mutex.acquire()
                state = self._tool.states.pop(0)
                self.mutex.release()
                action = self.send_state(state)
                if isinstance(action, dict) and 'action' in action.keys():
                    if action['action'] != "none":
                        if hasattr(self._tool, action['action'] + '_action'):
                            func = getattr(self._tool, action['action'] + '_action')
                            argspec = inspect.getargspec(func)
                            args = argspec.args
                            if len(args) == 1:
                                func()
                            elif len(args) == 2:
                                func(action['data'])
                            else:
                                raise Exception("action处理函数参数不能超过2个(包括self)!")
                        else:
                            self._tool.logger.warn("没有为返回action %s设置处理函数!" % action['action'])
                if state.name in {"finish", "error"}:
                    break
            else:
                action = self.send_state(State('keepalive', platform.uname()[1]))
                if isinstance(action, dict) and 'action' in action.keys():
                    if action['action'] != "none":
                        if hasattr(self._tool, action['action'] + '_action'):
                            func = getattr(self._tool, action['action'] + '_action')
                            argspec = inspect.getargspec(func)
                            args = argspec.args
                            if len(args) == 1:
                                func()
                            elif len(args) == 2:
                                func(action['data'])
                            else:
                                raise Exception("action处理函数参数不能超过2个(包括self)!")
                        else:
                            self._tool.logger.warn("没有为返回action %s设置处理函数!" % action['action'])
            gevent.sleep(int(self.config.KEEP_ALIVE_TIME))

    def send_state(self, state):
        """
        发送State信息到远程Actor接收

        :param state: State对象
        :return:
        """
        msg = {"id": self._tool.id,
               "state": state.name,
               "data": state.data
               }
        client = None
        try:
            client = zerorpc.Client()
            client.connect(self.config.endpoint)
            result = client.report(msg)
            client.close()
        except Exception, e:
            self._lost_connection_count += 1
            if self._lost_connection_count >= 10:
                self._tool.logger.error("网络连接出现错误，尝试10次仍然无法连接，即将退出运行:%s" % e)
                self._tool.exit_signal = True
                os.system("kill -9 %s" % os.getpid())
            else:
                self._tool.logger.error("网络连接出现错误，将重新尝试连接:%s" % e)
                if client:
                    client.close()
                gevent.sleep(3)
                self.send_state(state)
        else:
            if not isinstance(result, dict):
                self._tool.logger.error("接收到异常信息，退出运行!")
                sys.exit(1)
            self._lost_connection_count = 0
            return result

    def check_command(self):
        while not self._tool.is_end:
            if (not self.main_thread.is_alive()) or self._tool.exit_signal:
                break
            for name, cmd in self._tool.commands.items():
                if name not in self._has_record_commands:
                    gevent.spawn(self._tool.resource_record, cmd)
                    self._has_record_commands.append(name)
            gevent.sleep(3)
