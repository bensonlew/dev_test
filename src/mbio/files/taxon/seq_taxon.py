# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""Taxon格式文件类"""

from biocluster.iofile import File
import re
from biocluster.core.exceptions import FileError


class SeqTaxonFile(File):
    """
    Taxon文件格式类
    """
    def __init__(self, filename):
        """
        """
        super(SeqTaxonFile, self).__init__(filename)

    def get_info(self):
        """
        获取文件属性
        :return:
        """
        super(SeqTaxonFile, self).get_info()
        info = self.get_fileinfo()
        self.set_property("form", info[0])
        self.set_property("seq_num", info[1])

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        if super(SeqTaxonFile, self).check():
            if self.prop['form']:
                pass
            else:
                raise FileError(u"文件格式错误")
        return True

    def get_fileinfo(self):
        """
        获取物种分类文件信息
        """
        form, seq_num = True, 0
        with open(self.prop['path'], 'r') as f:
            while 1:
                line = f.readline().rstrip()
                if not line:
                    break
                taxline = re.split(r'\s', line)[1]
                taxs = re.split(r';\s*', taxline)
                for t in taxs:
                    if re.match(r'[dkpcofgs]\_\_\S+', t):
                        pass
                    else:
                        form = False
                        break
                seq_num += 1
                if not line:
                    break
        return (form, seq_num)
