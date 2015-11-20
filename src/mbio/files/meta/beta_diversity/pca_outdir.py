# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
# import re
# import subprocess
# from biocluster.config import Config
import os
import copy
from biocluster.core.exceptions import FileError


class PcaOutdirFile(Directory):
    """
    定义PCA分析输出文件夹类型
    """

    def __init__(self):
        super(PcaOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' not in self.prop.keys():
            raise FileError('请先设置文件夹路径！')
        if not os.path.isdir(self.prop['path']):
            raise FileError('文件夹路径不正确!')
        dirinfo = self.get_pca_info()
        dirpath = self.prop['path'].rstrip('/') + '/'
        self.set_property('sites_file', dirpath + dirinfo[2][2])
        self.set_property('ortation_file', dirpath + dirinfo[2][1])
        self.set_property('PC_imp_file', dirpath + dirinfo[2][0])
        self.set_property('sample', dirinfo[0])
        self.set_property('PC', dirinfo[1])
        self.set_property('env_set', False)
        if dirinfo[2][3]:
            self.set_property('PC', True)
            self.set_property('envfit_file', dirpath + dirinfo[2][5])
            self.set_property('envfit_score_file', dirpath + dirinfo[2][4])
            self.set_property('env_list', dirinfo[3])

    def get_pca_info(self):
        """
        获取并返回PCA输出文件夹信息

        :return sample,PC: 返回样品名列表，PC权重值，
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
        env = []
        if filelist[3]:
            temp_env = open(dirpath + filelist[4])
            env_lines = temp_env.readlines()
            env = [i.split()[0] for i in env_lines[1:]]
        return sample, PC, filelist, env

    def check(self):
        """
        检测文件夹是否满足要求，发生错误时应该触发FileError异常

        :return filelist: 返回所有文件名以及是否有环境因子
        """
        if super(PcaOutdirFile, self).check():
            # 父类check方法检查
            filelist = self.get_filesname()
            dirpath = self.prop['path'].rstrip('/')

            pca_importance = File()
            pca_importance.set_path(dirpath + filelist[0])
            pca_importance.check()
            pca_sites = File()
            pca_sites.set_path(dirpath + filelist[2])
            pca_sites.check()
            pca_rotation = File()
            pca_rotation.set_path(dirpath + filelist[1])
            pca_rotation.check()
            if filelist[3]:
                envfit = File()
                envfit.set_path(dirpath + filelist[5])
                envfit.check()
                envfit_score = File()
                envfit_score.set_path(dirpath + filelist[4])
                envfit_score.check()
            return filelist

    def get_filesname(self):
        """
        获取并检查文件夹下的文件是否存在且唯一。

        :return pca_importance_file, pca_rotation_file,
        pca_sites_file, env_set, pca_envfit_score_file,
        pca_envfit_file: 返回各个文件，以及是否存在环境因子，
        存在则返回环境因子结果
        """
        filelist = os.listdir(self.prop['path'])
        if len(filelist) != 6 and len(filelist) != 8:
            pass  # 如果文件数量有问题，应该给出提示信息，不影响程序，这里省略

        pca_importance = 0
        pca_rotation = 0
        pca_sites = 0
        pca_envfit_score = 0
        pca_envfit = 0
        pca_importance_file = ''
        pca_rotation_file = ''
        pca_sites_file = ''
        pca_envfit_score_file = ''
        pca_envfit_file = ''
        env_set = False
        for name in filelist:
            if 'pca_importance.xls' in name:
                pca_importance += 1
                pca_importance_file = name
            elif 'pca_sites.xls' in name:
                pca_sites += 1
                pca_sites_file = name
            elif 'pca_rotation.xls' in name:
                pca_rotation += 1
                pca_rotation_file = name
            elif 'pca_envfit_score.xls' in name:
                pca_envfit_score += 1
                pca_envfit_score_file = name
            elif 'pca_envfit.xls' in name:
                pca_envfit += 1
                pca_envfit_file = name
        if pca_importance != 1:
            raise FileError('*pca_importance.xls文件不存在或存在多组数据')
        elif pca_rotation != 1:
            raise FileError('*pca_rotation.xls文件不存在或存在多组数据')
        elif pca_sites != 1:
            raise FileError('*pca_sites.xls文件不存在或存在多组数据')
        else:
            if pca_envfit_score != 1 and pca_envfit == 1:
                raise FileError('环境因子结果数据缺失或存在多组数据')
            elif pca_envfit_score == 1 and pca_envfit != 1:
                pass
            elif pca_envfit_score == 1 and pca_envfit == 1:
                env_set = True
            else:
                pass
        if env_set:
            return (pca_importance_file, pca_rotation_file,
                    pca_sites_file, env_set, pca_envfit_score_file,
                    pca_envfit_file)
        else:
            return (pca_importance_file, pca_rotation_file,
                    pca_sites_file, env_set)
