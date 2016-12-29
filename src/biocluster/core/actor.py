# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""actor消息处理机制"""

import gevent
from gevent import Greenlet
import datetime
import threading
import inspect
import zerorpc
import platform
import traceback


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
        self.auto_break = False  # 自动退出
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
                if self._agent.job.submit_time is not None:
                        if (now - self._agent.job.submit_time).seconds > self._config.MAX_WAIT_TIME:
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
        workflow = self._agent.get_workflow()
        try:
            if not workflow.sheet.instant and self._update is None:
                self._agent.fire('runstart', message["data"])
            self._update = datetime.datetime.now()
            if (not isinstance(message, dict)) or ('state' not in message.keys()):
                self._agent.logger.warning("接收到不符合规范的消息，丢弃!")
            if message['state'] != "keepalive":
                self._agent.fire("recivestate", message)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            workflow.exit(exitcode=1, data=e)

    def _run(self):
        self._start_time = datetime.datetime.now()
        self.running = True
        while self.running:
            # message = self.inbox.get()
            # self.receive(message)
            self.check_time()
            if self._agent.is_end or self.auto_break:
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
        self.mutex = tool.mutex
        self._lost_connection_count = 0
        self.main_thread = main_thread
        self._has_record_commands = []

    def run(self):
        """
        定时检测Tool的State状态，并发送状态到远端

        :return: None
        """
        gevent.spawn(self.check_command)
        # is_end = self._tool.is_end
        # states = self._tool.states

        def is_finished():
            with self.mutex:
                if len(self._tool.states) > 0:
                    return False
                elif self._tool.is_end:
                    return True
                else:
                    return False

        while is_finished():
            with self.mutex:
                if self._tool.exit_signal and len(self._tool.states) == 0:
                    self._tool.logger.debug("接收到退出信号，终止Actor信号发送!")
                    self._tool.exit(0)
                    break
            if not self.main_thread.is_alive() and len(self._tool.states) == 0 \
                    and self._tool.exit_signal is not True:
                self.send_state(State('error', "检测到远程主线程异常结束"))
                self._tool.logger.debug("检测到主线程已退出，终止运行!")
                self._tool.exit(1)
                break
            if len(self._tool.states) > 0:
                with self.mutex:
                    state = self._tool.states.pop(0)
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
                    self._tool.logger.debug("state name: %s " % state.name)
                    self._tool.exit(0)
                    break
            else:
                if not self._tool.instant:
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
               "data": state.data,
               "version": self._tool.version
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
                self._tool.exit(1)
            else:
                self._tool.logger.error("网络连接出现错误，将重新尝试连接:%s" % e)
                if client:
                    client.close()
                gevent.sleep(3)
                self.send_state(state)
        else:
            if not isinstance(result, dict):
                self._tool.logger.error("接收到异常信息，退出运行!")
                self._tool.exit(1)
            self._lost_connection_count = 0
            return result

    def check_command(self):
        while not self._tool.is_end:
            if not self.main_thread.is_alive():
                break
            if self._tool.is_end or self._tool.exit_signal:
                break
            for name, cmd in self._tool.commands.items():
                if name not in self._has_record_commands:
                    gevent.spawn(self._tool.resource_record, cmd)
                    self._has_record_commands.append(name)
            gevent.sleep(3)


# class ProcessActor(RemoteActor):
#     def __init__(self, tool, main_thread):
#         super(ProcessActor, self).__init__(tool, main_thread)
#
#     def run(self):
#         self.config.KEEP_ALIVE_TIME = 1
#         super(ProcessActor, self).run()
#
#     def send_state(self, state):
#
#         msg = {"id": self._tool.id,
#                "state": state.name,
#                "data": state.data,
#                "version": self._tool.version
#                }
#         try:
#             self._tool.logger.debug("put msg %s" % msg)
#             self._tool.process_queue.put(msg)
#         except Exception, e:
#             self._tool.logger.debug("error: %s", e)
#
#         # print "Put MSG:%s" % msg
#         key = "%s" % self._tool.version
#         if key in self._tool.shared_callback_action.keys():
#             action = self._tool.shared_callback_action[key]
#             del self._tool.shared_callback_action[key]
#         else:
#             action = {'action': 'none'}
#         return action
