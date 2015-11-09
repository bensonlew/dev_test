# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File
# import re
# import subprocess
# from biocluster.config import Config
import os
import copy
from biocluster.core.exceptions import FileError


class DistanceMatrixFile(File):
    """
    定义DistanceMatrix文件
    """
    METHOD = ['abund_jaccard','binary_chisq','binary_chord',
              'binary_euclidean','binary_hamming','binary_jaccard',
              'inary_lennon','binary_ochiai','binary_otu_gain',
              'binary_pearson','binary_sorensen_dice',
              'bray_curtis','bray_curtis_faith','bray_curtis_magurran',
              'canberra','chisq','chord','euclidean','gower',
              'hellinger','kulczynski','manhattan','morisita_horn',
              'pearson','soergel','spearman_approx','specprof',
              'unifrac','unifrac_g','unifrac_g_full_tree',
              'unweighted_unifrac','unweighted_unifrac_full_tree',
              'weighted_normalized_unifrac','weighted_unifrac']
    # 现有的矩阵计算类型

    def __init__(self):
        super(DistanceMatrixFile, self).__init__()
        

    def get_info(self):
        """
        获取文件属性
        """
        super(DistanceMatrixFile, self).get_info()
        distancematrixinfo = self.get_matrix_info()
        self.set_property('method',distancematrixinfo[0])
        self.set_property('samp_numb',len(distancematrixinfo[1]))
        self.set_property('samp_list',distancematrixinfo[1])

    def get_matrix_info(self):
        """
        获取并返回矩阵信息
        :return:
        """
        method = self.get_method()
        tempfile = open(self.prop['path'])
        sample_list = tempfile.readline().strip('\n').split('\t')[1:]
        tempfile.close()
        return method,sample_list

    def get_method(self):
        """
        根据提供文件名获取开头的矩阵计算方法
        :return:
        """
        method = os.path.basename(self.prop['path'])  # 获取文件名
        method = method.split('_')
        if len(method) < 2: 
            method = 'unknown_method'
            return method
        if method[0] in DistanceMatrixFile.METHOD:  # 检查是否为单个单词的方法
            method = method[0]
        elif '_'.join([method[0],method[1]]) in DistanceMatrixFile.METHOD:  # 检查是否为两个个单词的方法
            method = '_'.join([method[0],method[1]])
        else :
            method = 'unknown_method'
        return method

    def get_value(self,row = 0,column = 0,SN = True):
        """
        依据row和column返回特定值或者名称
        SN(sequence number顺序号)说明row和column为行号和列号数字
        :param row:  行名或行号
        :param column:  列名或列号
        :param SN:  row和column为顺序号
        :param return:
        """
        tempfile = open(self.prop['path'])
        lines = tempfile.readlines()
        lines = [line.strip('\n') for line in lines]
        if not SN :
            row = lines[0].split('\t').index(row)  # 没有设置检查，可能row不存在
            column = lines[0].split('\t').index(column)
        value = lines[row].split('\t')[column]
        tempfile.close()
        return value



    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(DistanceMatrixFile,self).check():
        # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            for n in range(self.prop['samp_numb'] + 1):
                for m in range(self.prop['samp_numb'] + 1):
                    if self.get_value(n,m) != self.get_value(m,n):
                        raise FileError(u'距离矩阵格式不正确')
            if len(self.prop['samp_list']) != len(set(self.prop['samp_list'])):
                raise FileError(u'存在重复的样本名')

    def choose(self,sample_list = [],Except = False,path ='unknown'):
        """
        选择部分样本生成新的矩阵
        :param samp_list:  一个样品名称列表
        :param except:  除去samp_list列表的样本
        :return:
        """
        if Except:
            keep_samp = copy.deepcopy(self.prop['samp_list'])
            for i in sample_list:  # 
                keep_samp.remove(i)
        else :
            keep_samp = sample_list
        if path == 'unknown':
            filename = os.path.basename(self.prop['path']).split('.')
            dirname = os.path.dirname(self.prop['path'])
            if len(filename) > 1:
                path = dirname + '/' + filename[0] + '_new.' + filename[1]
            else :
                path = dirname + '/' + filename[0] + '_new'
        self.create_new(keep_samp,path)
        newmatrix = DistanceMatrixFile()
        newmatrix.set_path(path)
        return newmatrix

    def create_new(self,samp_list,path):
        """
        根据样品名和路径，创建一个新的矩阵文件
        :param samp_list:  样品名列表
        :param path:  新文件路径
        """
        newfile = open(path,'w')
        samp_list.insert(0,'')
        # indexlist = [self.prop['samp_list'].index(samp) + 1 for samp in samp_list]
        for m in samp_list:
            line = '\t'.join([self.get_value(m,n,SN = False) for n in samp_list]) + '\n'
            newfile.write(line)
