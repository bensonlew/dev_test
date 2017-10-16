# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
from biocluster.iofile import File
from biocluster.core.exceptions import FileError
from biocluster.config import Config
import os
import codecs
import chardet
import random
import re
import subprocess

class SampleBaseTableFile(File):
    """
    定义样本集的相关信息表格
    """
    def __init__(self):
        super(SampleBaseTableFile, self).__init__()
        self.platform = ''  # 测序平台
        self.strategy = ''  # 测序策略
        self.primer = ''   # 引物
        self.contract_number = ''  # 合同号
        self.contract_sequence_number = ''  # 签订测序量
        self.mj_number = ''  # 美吉编号
        self.client_name = ''  # 用户名

        # 写一些需要用到的数据的定义

    def get_info(self):
        """
            获取文件的基本属性
            """
        super(SampleBaseTableFile, self).get_info()
        self.set_property("platform", self.platform)
        self.set_property("strategy", self.strategy)
        self.set_property("primer", self.primer)
        self.set_property("contract_number", self.contract_number)
        self.set_property("contract_sequence_number", self.contract_sequence_number)
        self.set_property("mj_number", self.mj_number)
        self.set_property("client_name", self.client_name)

    def check_info(self):
        with open(self.prop['path'], 'r') as f:
            platform = []
            strategy = []
            primer = []
            contract_number = []
            contract_sequence_number = []
            mj_number = []
            client_name = []
            lines = f.readlines()
            for line in lines[1:]:
                tmp = line.strip().split("\t")
                platform.append(tmp[2])
                strategy.append(tmp[3])
                primer.append(tmp[4])
                contract_number.append(tmp[5])
                contract_sequence_number.append(tmp[6])
                mj_number.append(tmp[7])


    def check(self):
        if super(SampleBaseTableFile, self).check():
            self.get_info()
            return True


