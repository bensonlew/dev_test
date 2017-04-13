# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""rpc远程客户端和服务端"""

import zerorpc
from .config import Config
import datetime
import gevent
from zmq import ZMQError
from multiprocessing import Queue
import os
# import gipc


class Report(object):
    """
    RPCServer
    """
    def __init__(self, workflow):
        self.workflow = workflow

    def report(self, msg):
        """
        用于传递消息的方法。

        :param msg: 通过RPC服务接收的远程消息
        """

        agent = self.workflow.find_tool_by_id(msg['id'])
        if not agent:
            self.workflow.logger.error("Server在workflow中找不到对应的tool: {} !".format(msg['id']))
            agent.send_exit_action("Server在workflow中找不到对应的tool: {} !".format(msg['id']), msg["version"])
        if (not isinstance(msg, dict)) or ('id' not in msg.keys()) or ("version" not in msg.keys()):
            self.workflow.logger.error("Server接收到不符合规范的消息: 不是字典类型或没有key值'id'或者'version'!")
        elif msg["version"] != agent.version:
            self.workflow.logger.error("接收到已经重新投递任务的历史版本信号，丢弃: %s ！" % msg)
            agent.send_exit_action("此版本号已经更新过期", msg["version"])
        else:
            self.workflow.last_update = datetime.datetime.now()
            agent.actor.receive(msg)
        if not self.workflow.sheet.instant:
            return agent.get_callback_action(msg['version'])


class RPC(object):
    def __init__(self, workflow):
        self._rpc_server = zerorpc.Server(Report(workflow))
        config = Config()
        self.endpoint = "tcp://{}:{}".format(config.LISTEN_IP, config.LISTEN_PORT)
        try:
            self._rpc_server.bind(self.endpoint)
        except ZMQError:
            workflow.logger.info("Workflow 端口绑定失败,重新绑定,旧端口:{}".format(self.endpoint))
            self.endpoint = "tcp://{}:{}".format(config.LISTEN_IP, config.LISTEN_PORT)
            workflow.logger.info("新端口:{}".format(self.endpoint))
            self._rpc_server.bind(self.endpoint)

    def run(self):
        """
        开始运行RPC监听,此时会阻塞线程
        """
        self._rpc_server.run()

    def close(self):
        self._rpc_server.close()


class LocalServer(object):
    def __init__(self, workflow):
        self._report = Report(workflow)
        self._close = False
        # (reader, writer) = gipc.pipe()
        # print "reader %s, writer %s" % (reader, writer)
        self.process_queue = Queue()
        self.endpoint = ""

    def run(self):
        while True:
            gevent.sleep(0.3)
            try:
                msg = self.process_queue.get_nowait()
            except Exception:
                pass
            else:
                if msg:
                    self._report.report(msg)
            if self._close:
                break

    def close(self):
        self._close = True


#
# class RPCClient(zerorpc.Client):
#     """
#     sent message of each tool running states
#     发送tool的状态
#     """
#     def __init__(self, endpoint, tool):
#         """
#         link to a server.
#         连接到一个服务端
#         """
#         super(RPCClient, self).__init__()
#         self.connect(endpoint)
#         self.tool = tool
#
#     def run(self, msg):
#         """
#         sent message to server.
#         发送消息给服务端, 并获得返回的消息，将获得的消息告诉Tool
#         """
#         try:
#             feedback = self.rpc_letter(msg)
#         except Exception, e:
#             self._logger.error("Client 运行出现错误: {}".format(e))
#         else:
#             if feedback is None:
#                 pass
#                 self._logger.info("Client 没有反馈消息给tool")
#             elif feedback[0] == "error":
#                 self.tool.kill()
#                 self._logger.info("Client 接到终止tool运行消息，终止{}".format(self.tool))
#             else:
#                 getattr(self.tool, feedback[1])
#                 self._logger.info("Client 传递消息给{}执行：{}".format(self.tool, feedback[1]))
