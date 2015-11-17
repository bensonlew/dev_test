# encoding: utf-8

"""workflow工作流类模块"""

from .core.function import load_class_by_path
from .basic import Basic
from .config import Config
import os
import sys
from .rpc import RPC
from .logger import Wlog
from .agent import Agent
import datetime
import gevent


class Workflow(Basic):
    """
    工作流程基类
    """

    def __init__(self, work_id):
        super(Workflow, self).__init__()
        self._id = work_id
        self.config = Config()
        self._work_dir = self.__work_dir()
        self._output_path = self._work_dir + "/output"
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
        self._logger = Wlog(self).get_logger(self._id)
        self.rpc_server = RPC(self)
        self.db = self.config.get_db()

    def __work_dir(self):
        """
        获取并创建工作目录
        """
        work_dir = self.config.WORK_DIR
        work_dir = work_dir + "/" + self.name + self._id
        if not os.path.exists(work_dir):
            os.makedirs(work_dir)
        return work_dir

    def add_module(self, path):
        """
        添加下属 :py:class:`biocluster.module.Module`

        :param path:  :py:class:`biocluster.module.Module` 对应的自动加载path路径，请参考教程中对应的说明

        """
        module = load_class_by_path(path, tp="Module")(self)
        self.add_child(module)
        return module

    def add_tool(self, path):
        """
        直接添加下属 :py:class:`biocluster.agent.Agent`

        :param path: String   :py:class:`biocluster.agent.Agent` 动态加载path路径
        :return:  Agent 返回Tool对应的 :py:class:`biocluster.agent.Agent` 对象
        """
        tool = load_class_by_path(path, tp="Agent")(self)
        self.add_child(tool)
        return tool

    def find_tool_by_id(self, toolid):
        """
        通过id搜索所属Tool

        :param toolid:  :py:class:`biocluster.tool.Tool` 对象的ID
        """
        ids = toolid.split(".")
        length = len(ids)
        if length < 2 or length > 3:
            return False
        if ids[0] != self.id:
            return False
        modules = self.children
        if length == 2:
            for md in modules:
                if md.id == (ids[0] + "." + ids[1]) and isinstance(md, Agent):
                    return md
        elif length == 3:
            for module in modules:
                tool = module.find_tool_by_id(toolid)
                if tool:
                    return tool
        return False

    def run(self):
        """
        开始运行

        :return:
        """
        super(Workflow, self).run()
        if self.config.USE_DB:
            db = self.config.get_db()
            data = {
                "last_update": datetime.datetime.now(),
                "workdir": self.work_dir
            }
            db.update("workflow", where="workflow_id = %s" % self._id, **data)
            gevent.spawn(self.__update_database)
        self.rpc_server.run()

    def end(self):
        """
        停止运行
        """
        super(Workflow, self).end()
        if self.config.USE_DB:
            db = self.config.get_db()
            data = {
                "is_end": 1,
                "end_time": datetime.datetime.now(),
                "workdir": self.work_dir,
                "output": self.output_dir
            }
            db.update("workflow", where="workflow_id = %s" % self._id, **data)

        self.rpc_server.server.close()
        self.logger.info("运行结束!")

    def exit(self, exitcode=1, data=""):
        """
        立即退出当前流程

        :param exitcode:
        :return:
        """
        self.rpc_server.server.close()
        if self.config.USE_DB:
            db = self.config.get_db()
            data = {
                "is_error": 1,
                "error": "程序主动退出:%s" % data,
                "end_time": datetime.datetime.now(),
                "is_end": 1,
                "workdir": self.work_dir,
                "output": self.output_dir
            }
            db.update("workflow", where="workflow_id = %s" % self._id, **data)
        sys.exit(exitcode)

    def __update_database(self):
        """
        每隔30s定时更新数据库last_update时间

        :return:
        """
        while self.is_end is False:
            gevent.sleep(30)
            data = {
                "last_update": datetime.datetime.now()
            }
            self.db.update("workflow", where="workflow_id = %s" % self._id, **data)
            results = self.db.query("SELECT * FROM tostop WHERE workflow_id=$workflow_id and done  = 0",
                                    vars={'workflow_id': self._id})
            if len(results) > 0:
                data = results[0]
                update_data = {
                    "stoptime": datetime.datetime.now(),
                    "done": 1
                }
                self.db.update("tostop", where="workflow_id = %s" % self._id, **update_data)
                self.exit(data=data.reson)


















