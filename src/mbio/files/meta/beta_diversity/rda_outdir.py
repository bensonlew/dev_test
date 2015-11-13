# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
from biocluster.core.exceptions import FileError


class RdaOutdirFile(Directory):
    """
    定义rda分析输出文件夹类型
    """

    def __init__(self):
        super(RdaOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' not in self.prop.keys():
            raise FileError('请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError('文件夹路径不正确!')
        dirinfo = self.get_rda_info()
        dirpath = self.prop['path'].rstrip('/') + '/'
        self.set_property('sites_file', dirpath + dirinfo[6][2])
        self.set_property('species_file', dirpath + dirinfo[6][1])
        self.set_property('imp_file', dirpath + dirinfo[6][0])
        self.set_property('dca_file', dirpath + dirinfo[6][3])
        self.set_property('env_file', dirpath + dirinfo[6][4])
        self.set_property('sample', dirinfo[0])
        self.set_property('species', dirinfo[1])
        self.set_property('importance', dirinfo[2])
        self.set_property('env', dirinfo[3])
        self.set_property('dca', dirinfo[4])
        self.set_property('method', dirinfo[5])

    def get_rda_info(self):
        """
        获取并返回rda输出文件夹信息

        :return sample, species, importance,
        env, dca, method, filelist: 返回样品名列表，PC权重值，和文件列表
        """
        filelist = self.get_filesname()
        dirpath = self.prop['path'].rstrip('/') + '/'
        temp_sites = open(dirpath + filelist[2])
        sites_lines = temp_sites.readlines()
        sample = [i.split()[0] for i in sites_lines[1:]]
        temp_importance = open(dirpath + filelist[0])
        imp_lines = temp_importance.readlines()
        importance = [(i.rstrip().split()[0], i.rstrip().split()[1])
                      for i in imp_lines[1:]]
        temp_env = open(dirpath + filelist[4])
        env_lines = temp_env.readlines()
        env = [i.split()[0] for i in env_lines[1:]]
        temp_dca = open(dirpath + filelist[3])
        dca = float(temp_dca.readlines()[-1].strip().split()[1])
        if dca >= 3.5:
            method = 'cca'
        else:
            method = 'rda'
        temp_sp = open(dirpath + filelist[1])
        species = [i.split()[0] for i in temp_sp.readlines()[1:]]
        return sample, species, importance, env, dca, method, filelist

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常

        :return bool: 检查完成
        """
        if super(RdaOutdirFile, self).check():
            # 父类check方法检查
            filelist = self.get_filesname()
            dirpath = self.prop['path'].rstrip('/')
            importance = File()
            importance.set_path(dirpath + filelist[0])
            importance.check()
            sites = File()
            sites.set_path(dirpath + filelist[2])
            sites.check()
            species = File()
            species.set_path(dirpath + filelist[1])
            species.check()
            dca = File()
            dca.set_path(dirpath + filelist[3])
            dca.check()
            environment = File()
            environment.set_path(dirpath + filelist[4])
            environment.check()
            return True

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在且唯一。

        :return importance_file, species_file, sites_file,
        dca_file, environment_file: 返回各个文件
        """
        filelist = os.listdir(self.prop['path'])
        importance = 0
        species = 0
        sites = 0
        dca = 0
        environment = 0
        importance_file = ''
        species_file = ''
        sites_file = ''
        dca_file = ''
        environment_file = ''
        for name in filelist:
            if '_importance.xls' in name:
                importance += 1
                importance_file = name
            elif '_sites.xls' in name:
                sites += 1
                sites_file = name
            elif '_species.xls' in name:
                species += 1
                species_file = name
            elif '_dca.txt' in name:
                dca += 1
                dca_file = name
            elif '_environment.xls' in name:
                environment += 1
                environment_file = name
            else:
                pass
        if importance != 1:
            raise FileError('*importance.xls文件不存在或存在多组数据')
        elif species != 1:
            raise FileError('*species.xls文件不存在或存在多组数据')
        elif sites != 1:
            raise FileError('*sites.xls文件不存在或存在多组数据')
        elif dca != 1:
            raise FileError('*dca.txt文件不存在或存在多组数据')
        elif environment != 1:
            raise FileError('*environment.xls文件不存在或存在多组数据')
        else:
            pass
        return (importance_file, species_file, sites_file, dca_file, environment_file)
