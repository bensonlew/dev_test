# -*- coding: utf-8 -*-

# __author__ = 'hongdongxuan'
# time: 2017/

import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class PpiFile(File):
    """
    as文件夹格式
    """
    def __init__(self):
        super(PpiFile, self).__init__()

    def check(self):
        if super(PpiFile, self).check():
            return True