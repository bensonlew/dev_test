# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config


class FileCheckAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.02.17
    用于在workflow开始之前对所输入的文件进行详细的内容检测
    """
    super(FileCheckAgent, self).__init__(parent)
    options = [
        {"name": "in_fastq", "type": "infile", 'format': "sequence.fastq, sequence.fastq_dir"}
        {"name": }
    ]
