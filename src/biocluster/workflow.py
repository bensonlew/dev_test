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
import time


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
        self._logger = Wlog(self).get_logger("")
        self._logger = Wlog(self).get_logger("")
        self.rpc_server = RPC(self)
        self.db = self.config.get_db()

    def __work_dir(self):
        """
        获取并创建工作目录
        """
        work_dir = self.config.WORK_DIR
        timestr = str(time.strftime('%Y%m%d', time.localtime(time.time())))
        work_dir = work_dir + "/" + timestr + "/" + self.name + self._id
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
        data = {
            "last_update": datetime.datetime.now(),
            "workdir": self.work_dir
        }
        self._update(data)
        if self.config.USE_DB:
            gevent.spawn(self.__update_service)
            gevent.spawn(self.__check_tostop)
        self.rpc_server.run()

    def end(self):
        """
        停止运行
        """
        super(Workflow, self).end()

        data = {
            "is_end": 1,
            "end_time": datetime.datetime.now(),
            "workdir": self.work_dir,
            "output": self.output_dir
        }
        self._update(data)

        self.rpc_server.server.close()
        self.logger.info("运行结束!")

    def exit(self, exitcode=1, data=""):
        """
        立即退出当前流程

        :param exitcode:
        :return:
        """
        self.rpc_server.server.close()
        data = {
            "is_error": 1,
            "error": "程序主动退出:%s" % data,
            "end_time": datetime.datetime.now(),
            "is_end": 1,
            "workdir": self.work_dir,
            "output": self.output_dir
        }
        self._update(data)
        sys.exit(exitcode)

    def __update_service(self):
        """
        每隔30s定时更新数据库last_update时间

        :return:
        """
        while self.is_end is False:
            gevent.sleep(15)
            try:
                self.db.query("UPDATE workflow SET last_update=CURRENT_TIMESTAMP where workflow_id=$id",
                              vars={'id': self._id})
            except Exception, e:
                self.logger.info("数据库更新异常: %s" % e)

    def _update(self, data):
        """
        插入数据库，更新流程运行状态,只在后台服务调用时生效

        :param data: 需要更新的数据
        :return:
        """
        if self.config.USE_DB:
            myvar = dict(id=self._id)
            self.db.update("workflow", vars=myvar, where="workflow_id = $id", **data)

    def __check_tostop(self):
        """
        检查数据库的停止指令，如果收到则退出流程

        :return:
        """
        while self.is_end is False:
            gevent.sleep(15)
            myvar = dict(id=self._id)
            try:
                results = self.db.query("SELECT * FROM tostop WHERE workflow_id=$id and done  = 0", vars={'id': self._id})
                if len(results) > 0:
                    data = results[0]
                    update_data = {
                        "stoptime": datetime.datetime.now(),
                        "done": 1
                    }
                    self.db.update("tostop", vars=myvar, where="workflow_id = $id", **update_data)
                    self.exit(data=data.reson)
            except Exception, e:
                self.logger.info("查询数据库异常: %s" % e)


















