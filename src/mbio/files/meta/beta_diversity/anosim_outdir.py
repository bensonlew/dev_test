# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
import re
from biocluster.core.exceptions import FileError


class AnosimOutdirFile(Directory):
    """
    定义anosim分析输出文件夹类型
    """

    def __init__(self):
        super(AnosimOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' not in self.prop.keys():
            raise FileError('请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError('文件夹路径不正确!')
        self.format_result()
        self.set_property('method', ['anosim', 'adonis'])

    # def get_anosim_info(self):
    #     """
    #     获取并返回anosim输出文件夹信息
    #     """
    #     pass

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常
        """
        if super(AnosimOutdirFile, self).check():
            # 父类check方法检查
            filelist = os.listdir(self.prop['path'])
            if 'adonis_results.txt' not in filelist:
                raise FileError('没有adonis结果')
            if 'anosim_results.txt' not in filelist:
                raise FileError('没有anosim结果')
            dirpath = self.prop['path'].rstrip('/')
            anosim = File()
            anosim.set_path(dirpath + 'anosim_results.txt')
            anosim.check()
            adonis = File()
            adonis.set_path(dirpath + 'adonis_results.txt')
            adonis.check()

    def format_result(self):
        """

        """
        if self.prop.has_key('format'):
            if self.prop['format']:
                pass
            else:
                self.format()
        else:
            self.format()

    def format(self):
        """
        将‘adonis_results.txt’和‘anosim_results.txt’两个文件的内容
        整理写入到表格‘format_results.txt’中
        """
        dirpath = self.prop['path'].strip('/') + '/'
        an = open(dirpath + 'anosim_results.txt')
        ad = open(dirpath + 'adonis_results.txt')
        new = open(dirpath + 'format_results.txt', 'w')
        an_line = an.readlines()
        for line in ad:
            if re.match('qiime.data$map[[opts$category]]', line):
                ad_r = line.split()[5]
                ad_p = line.split()[6]
        sample = an_line[2].strip().split('\t')[1]
        groups_num = an_line[3].strip().split('\t')[1]
        an_r = an_line[4].strip().split('\t')[1]
        an_p = an_line[5].strip().split('\t')[1]
        permu = an_line[6].strip().split('\t')[1]
        new.write('method\tstatisic\tp-value\tnumber of permutation\n')
        new.write('anosim\t%s\t%s\t%s\n' % (an_r, an_p, permu))
        new.write('adonis\t%s\t%s\t%s\n' % (an_r, an_p, permu))
        new.close()
        ad.close()
        an.close()
        self.set_property('format', True)
        self.set_property('permutation', permu)
        self.set_property('groups_num', groups_num)
        self.set_property('sample_num', sample)
