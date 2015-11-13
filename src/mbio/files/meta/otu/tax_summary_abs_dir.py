# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory


class TaxSummyAbsDir(Directory):
    """
    定义tax_summary_abs_dir文件夹格式
    """
    def __init__(self):
        super(TaxSummyAbsDir, self).__init__()
        self.biom = 0
        self.otu_table = 0

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            self.set_file_number(14)
            self.get_file_number()
        else:
            raise FileError("文件夹路径不正确，请设置正确的文件夹路径!")

    def get_file_number(self):
        """
        获取文件夹下的biom文件和otu_table文件的数目
        :return:文件数目
        """
        filelist = os.listdir(self.prop['path'])
        if self.biom != 0:
            return (self.biom, self.otu_table)
        else:
            for file_ in filelist:
                if re.search(r'^otu_taxa_table_L\d\.otu_table$', file_):
                    self.otu_table += 1
                if re.search(r'^otu_taxa_table_L\d\.biom$', file_):
                    self.biom += 1
            return (self.biom, self.otu_table)

    def check(self):
        """
        检测文件夹是否满足要求，不满足时触发FileError异常
        """
        if super(TaxSummyAbsDir, self).check():
            if self.biom != 7 or self.otu_table != 7:
                raise FileError("文件夹里biom文件和self.otu_table文件数目不正确！")
