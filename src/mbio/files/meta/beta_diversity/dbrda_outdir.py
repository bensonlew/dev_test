# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
import re
from biocluster.core.exceptions import FileError


class DbrdaOutdirFile(Directory):
    """
    定义Dbrda分析输出文件夹类型
    """

    def __init__(self):
        print 'init'
        super(DbrdaOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        print 'getinfo'
        super(DbrdaOutdirFile, self).get_info()
        if 'path' not in self.prop.keys():
            raise FileError('请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError('文件夹路径不正确!')
        factor, pernum, sig, sample, filelist = self.get_dbrda_info()
        dirpath = self.prop['path'].rstrip('/') + '/'
        self.set_property('sites_file', dirpath + filelist[2])
        self.set_property('factor_file', dirpath + filelist[0])
        self.set_property('result_file', dirpath + filelist[1])
        self.set_property('sample', sample)
        self.set_property('factor', factor)
        self.set_property('Permutation', pernum)
        self.set_property('Significance', sig)
        print 'get all info'

    def get_dbrda_info(self):
        """
        获取环境因子，随机检测次数，显著性值，样品列表, 文件列表
        :return factor, pernum, sig, sample, filelist: 列表；数字或字符；数字或字符；列表
        """
        filelist = self.get_filesname()
        dirpath = self.prop['path'].rstrip('/') + '/'
        tempfactor = open(dirpath + filelist[0])
        lines = tempfactor.readlines()
        factor = [i.split()[0] for i in lines[1:]]
        tempfactor.close()
        pernum = 'unknown'
        sig = 'unknown'
        tempres = open(dirpath + filelist[1])
        for line in tempres:
            if 'Number of permutations:' in line:
                get_num = re.search(r'\s(\d+)\s', line)
                pernum = get_num.group(1)
                continue
            if 'Significance:' in line:
                get_sig = re.search(r'\s([\d\.]+)\s', line)
                sig = get_sig.group(1)
                continue
        tempres.close()
        tempsites = open(dirpath + filelist[2])
        sample = [i.split()[0] for i in tempsites]
        sample.pop(0)
        return factor, pernum, sig, sample, filelist

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常
        :return bool: 检查完成
        """
        if super(DbrdaOutdirFile, self).check():
            # 父类check方法检查
            filelist = self.get_filesname()
            dirpath = self.prop['path'].rstrip('/')
            factor = File()
            factor.set_path(dirpath + filelist[0])
            factor.check()
            sites = File()
            sites.set_path(dirpath + filelist[2])
            sites.check()
            result = File()
            result.set_path(dirpath + filelist[1])
            result.check()
            return True

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在且唯一。

        :return factor_file, results_file, sites_file: 返回各个文件
        """
        filelist = os.listdir(self.prop['path'])
        factor = 0
        result = 0
        sites = 0
        factor_file = ''
        result_file = ''
        sites_file = ''
        for name in filelist:
            if 'db_rda_factor.txt' in name:
                factor += 1
                factor_file = name
            elif 'db_rda_results.txt' in name:
                result += 1
                result_file = name
            elif 'db_rda_sites.txt' in name:
                sites += 1
                sites_file = name
            else:
                pass
        if factor != 1:
            raise FileError('db_rda_factor.txt文件不存在或存在多组数据')
        elif result != 1:
            raise FileError('db_rda_results.txt文件不存在或存在多组数据')
        elif sites != 1:
            raise FileError('db_rda_sites.txt文件不存在或存在多组数据')
        else:
            pass
        return factor_file, result_file, sites_file
