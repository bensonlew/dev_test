# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import File


class KeggTableFile(File):
    """
    定义kegg_table.xls格式
    """
    def __init__(self):
        super(KeggTableFile, self).__init__()

    def check(self):
        if super(KeggTableFile, self).check():
            return True
