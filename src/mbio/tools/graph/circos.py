# -*- coding: utf-8 -*-
# __author__ = zengjing
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class CircosAgent(Agent):
    """
    小工具弦图：对二维表格(目前二维表格只能是行为样本)进行合并
    """
