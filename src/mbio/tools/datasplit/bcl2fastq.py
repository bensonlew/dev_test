# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""bcl2fastq 工具 """

from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError


class Bcl2fastqAgent(Agent):
    """
    bcl2fastq
    version 2.17
    """
    def __init__(self, parent=None):
        super(Bcl2fastqAgent, self).__init__(parent)
        options = [
            {'name': 'file_path', 'type': "string"},  # 下机数据文件路径
            {'name': 'barcode_mismatch', 'type': 'int', 'default': 0},  # barcode错配数
            {'name': 'ignore_missing_bcl', 'type': bool, 'default': True},  # 是否忽略错配的bcl
            {'name': 'base_mask', 'type': "string", 'default': "y301,i6,y301"},  # base_mask的值，meta拆分的时候一般都是"y301,i6,y301"
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.parent_samples'}  # 父样本拆分信息表
        ]
