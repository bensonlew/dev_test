# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
from biocluster.core.exceptions import FileError


class NmdsOutdirFile(Directory):
    """
    定义nmds分析输出文件夹类型
    """

    def __init__(self):
        super(NmdsOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' not in self.prop.keys():
            raise FileError('请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError('文件夹路径不正确!')
        dirinfo = self.get_nmds_info()
        dirpath = self.prop['path'].rstrip('/') + '/'
        self.set_property('sites_file', dirpath + dirinfo[1])
        self.set_property('sample', dirinfo[0])

    def get_nmds_info(self):
        """
        获取并返回nmds输出文件夹信息

        :return sample, PC, filelist: 返回样品名列表，PC权重值，和文件列表
        """
        filelist = self.get_filesname()
        dirpath = self.prop['path'].rstrip('/') + '/'
        temp_sites = open(dirpath + filelist)
        sites_lines = temp_sites.readlines()
        sample = [i.split()[0] for i in sites_lines[1:]]
        return sample, filelist

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常

        :return bool: 检查完成
        """
        if super(NmdsOutdirFile, self).check():
            # 父类check方法检查
            filelist = self.get_filesname()
            dirpath = self.prop['path'].rstrip('/')
            nmds_sites = File()
            nmds_sites.set_path(dirpath + filelist)
            nmds_sites.check()
            return True

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在且唯一。

        :return nmds_importance_file, nmds_rotation_file,
        nmds_sites_file: 返回各个文件
        """
        filelist = os.listdir(self.prop['path'])
        nmds_sites = 0
        nmds_sites_file = ''
        for name in filelist:
            if 'nmds_sites.xls' in name:
                nmds_sites += 1
                nmds_sites_file = name
        if nmds_sites != 1:
            raise FileError('*nmds_sites.xls文件不存在或存在多组数据')
        return nmds_sites_file
