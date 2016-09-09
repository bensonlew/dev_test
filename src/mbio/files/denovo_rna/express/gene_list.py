# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class GeneListFile(File):
    """
    gene名字列表文件
    """
    def __init__(self):
        super(GeneListFile, self).__init__()

    def check(self):
        if super(GeneListFile, self).check():
            return True
