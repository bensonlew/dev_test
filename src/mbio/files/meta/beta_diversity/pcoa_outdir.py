# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
from biocluster.core.exceptions import FileError


class PcoaOutdirFile(Directory):
    """
    定义PCoA分析输出文件夹类型
    """

    def __init__(self):
        super(PcoaOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' not in self.prop.keys():
            raise FileError(u'请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError(u'文件夹路径不正确!')
        dirinfo = self.get_pcoa_info()
        dirpath = self.prop['path'].rstrip('/') + '/'
        self.set_property('sites_file', dirpath + dirinfo[2][2])
        self.set_property('ortation_file', dirpath + dirinfo[2][1])
        self.set_property('PC_imp_file', dirpath + dirinfo[2][0])
        self.set_property('sample', dirinfo[0])
        self.set_property('PC', dirinfo[1])

    def get_pcoa_info(self):
        """
        获取并返回pcoa输出文件夹信息

        :return sample, PC, filelist: 返回样品名列表，PC权重值，和文件列表
        """
        filelist = self.get_filesname()
        dirpath = self.prop['path'].rstrip('/') + '/'
        temp_sites = open(dirpath + filelist[2])
        sites_lines = temp_sites.readlines()
        sample = [i.split()[0] for i in sites_lines[1:]]
        temp_importance = open(dirpath + filelist[0])
        imp_lines = temp_importance.readlines()
        PC = [(i.rstrip().split()[0], i.rstrip().split()[1])
              for i in imp_lines[1:]]
        return sample, PC, filelist

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常

        :return bool: 检查完成
        """
        if super(PcoaOutdirFile, self).check():
            # 父类check方法检查
            filelist = self.get_filesname()
            dirpath = self.prop['path'].rstrip('/')

            pcoa_importance = File()
            pcoa_importance.set_path(dirpath + filelist[0])
            pcoa_importance.check()
            pcoa_sites = File()
            pcoa_sites.set_path(dirpath + filelist[2])
            pcoa_sites.check()
            pcoa_rotation = File()
            pcoa_rotation.set_path(dirpath + filelist[1])
            pcoa_rotation.check()
            return True

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在且唯一。

        :return pcoa_importance_file, pcoa_rotation_file,
        pcoa_sites_file: 返回各个文件
        """
        filelist = os.listdir(self.prop['path'])
        pcoa_importance = 0
        pcoa_rotation = 0
        pcoa_sites = 0
        pcoa_importance_file = ''
        pcoa_rotation_file = ''
        pcoa_sites_file = ''
        for name in filelist:
            if 'pcoa_importance.xls' in name:
                pcoa_importance += 1
                pcoa_importance_file = name
            elif 'pcoa_sites.xls' in name:
                pcoa_sites += 1
                pcoa_sites_file = name
            elif 'pcoa_rotation.xls' in name:
                pcoa_rotation += 1
                pcoa_rotation_file = name
            else:
                pass
        if pcoa_importance != 1:
            raise FileError(u'*pcoa_importance.xls文件不存在或存在多组数据')
        elif pcoa_rotation != 1:
            raise FileError(u'*pcoa_rotation.xls文件不存在或存在多组数据')
        elif pcoa_sites != 1:
            raise FileError(u'*pcoa_sites.xls文件不存在或存在多组数据')
        else:
            pass
        return (pcoa_importance_file, pcoa_rotation_file, pcoa_sites_file)
