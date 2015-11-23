# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory
from mbio.files.meta.otu.otu_table import OtuTableFile
from mbio.files.meta.otu.biom import BiomFile


class TaxSummaryAbsDirFile(Directory):
    """
    定义tax_summary_abs_dir文件夹格式
    """
    def __init__(self):
        super(TaxSummaryAbsDirFile, self).__init__()
        self.biom = 0
        self.otu_table = 0

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            pass
        else:
            raise FileError("文件夹路径不正确，请设置正确的文件夹路径!")

    def get_file_number(self):
        """
        获取文件夹下的biom文件和otu_table文件的数目
        :return:文件数目
        """
        filelist = os.listdir(self.prop['path'])
        for file_ in filelist:
            file_ = os.path.join(self.prop['path'], filelist)
            otu = OtuTableFile()
            otu.set_path(file_)
            biom = BiomFile()
            biom.set_path(file_)
            if otu.check():
                self.otu_table += 1
            if biom.check():
                self.biom += 1
        return (self.biom, self.otu_table)

    def check(self):
        """
        检测文件夹是否满足要求，不满足时触发FileError异常
        """
        if super(TaxSummaryAbsDirFile, self).check():
            pass
