# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

"""
功能基本类
"""
from .core.function import load_class_by_path
from .basic import Basic


class Module(Basic):
    """
    功能基本类
    """
    def __init__(self, parent):
        super(Module, self).__init__(parent)

    def add_tool(self, path):
        """
        添加下属工具，跳过模块

        :param path: String   Tool路径名称
        :return:  Agent 返回Tool对应的 :py:class:`biocluster.agent.Agent` 对象
        """
        agent = load_class_by_path(path, tp="Agent")(self)
        self.add_child(agent)
        return agent

    def find_tool_by_id(self, toolid):
        """
        通过id搜索所属Tool

        :param toolid:  :py:class:`biocluster.tool.Tool` 对象的ID
        """
        # ids = toolid.split(".")
        # if len(ids) < 3:
        #     return False
        # if (ids[0] + "." + ids[1]) != self.id:
        #     return False
        childs = self.children
        for c in childs:
            if c.id == toolid:
                return c
        return False

