# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""在备份完成之后，统计一次拆分和二次拆分的结果，生成json"""
import os
import time


class SplitStatAgent(Agent):
    """
    对数据拆分的结果进行统计并生成json文件
    """
    def __init__(self, parent=None):
        super(SplitStatAgent, self).__init__(parent)
        options = [
        
        ]
