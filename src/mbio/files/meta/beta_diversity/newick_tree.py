# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File
import re
# import subprocess
# from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class NewickTreeFile(File):
    """
    """

    def __init__(self):
        super(NewickTreeFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(NewickTreeFile, self).get_info()
        newickinfo = self.get_newick_info()
        self.set_property('sample', newickinfo)
        self.set_property('count', len(newickinfo))

    def get_newick_info(self):
        """
        获取并返回树文件信息
        :return:
        """
        tempfile = open(self.prop['path'])
        tree = tempfile.readlines()[0].rstrip()
        raw_samp = re.findall(r'(([\.0-9a-zA-Z_-]+):[0-9])', tree)
        samp = [i[1] for i in raw_samp]
        return samp

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(NewickTreeFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            tempfile = open(self.prop['path'])
            tree = tempfile.readlines()
            if len(tree) == 1:
                pass
            else:
                raise FileError(u'文件中存在多个newick树')
                # 可以保留第一个数，删除其他树，继续操作
            tree = tree[0].rstrip()
            if tree.count('(') == tree.count(')'):
                pass
            else:
                raise FileError(u'树文件格式错误')
            if len(re.findall(r':[\.0-9]+', tree)) == tree.count(':'):
                pass
            else:
                raise FileError(u'程序只接受带有分支距离的树文件，或者文件中距离表示错误')
            if tree[-1] == ';' and tree[-2] == ')':
                pass
            else:
                raise FileError(u'文件结尾不是分号‘;’')
