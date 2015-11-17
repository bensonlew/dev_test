# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""lefse_pdf格式文件类"""

from biocluster.iofile import File
import re
import subprocess
from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class LefsePdfFile(File):
    """
    定义LefsePdf文件
    """
    def __init__(self):
        super(LefsePdfFile,self).__init__()

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        super(LefsePdfFile,self).check()

    def get_info(self):
        """
        获取文件属性
        """
        super(LefsePdfFile, self).get_info()