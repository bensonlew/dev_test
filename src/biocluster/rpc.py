# encoding: utf-8
# __author__ = 'yuguo'

"""rpc远程客户端和服务端"""

import zerorpc
from .config import Config


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

        tool = self.workflow.find_tool_by_id(msg['id'])
        if not tool:
            self.workflow.logger.error(u"Server在workflow中找不到对应的tool: {} !".format(msg['id']))
        if (not isinstance(msg, dict)) or ('id' not in msg.keys()):
            self.workflow.logger.error(u"Server接收到不符合规范的消息: 不是字典类型或没有key值'id'!")
        else:
            tool.actor.receive(msg)
        return tool.get_callback_action()


class RPC(object):
    def __init__(self, workflow):
        self._rpc_server = zerorpc.Server(Report(workflow))
        config = Config()
        self.endpoint = "tcp://{}:{}".format(config.LISTEN_IP, config.LISTEN_PORT)
        self._rpc_server.bind(self.endpoint)

    @property
    def server(self):
        """
        获取zerorpc.Server对象
        """
        return self._rpc_server

    def run(self):
        """
        开始运行RPC监听,此时会阻塞线程
        """
        self.server.run()

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
