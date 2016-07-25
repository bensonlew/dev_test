# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class ControlTableFile(File):
    """
    """

    def __init__(self):
        super(ControlTableFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(ControlTableFile, self).get_info()
        self.get_control_dict()
        num, vs_list = self.get_control_info()
        if num == 0:
            raise Exception('对照方案至少为1')
        self.set_property('vs_list', vs_list)


    def get_control_info(self):
        """
        :return:对照样本（组）数目：num；包含两两比较的样本（分组）元组的列表
        """
        with open(self.prop['path'], 'rb') as r:
            lines = r.readlines()
            num = len(lines)-1
            vs_list = []
            for line in lines[1:]:
                vs = line.strip('\n').split()[0]
                if not re.findall(r'_vs_', vs):
                    raise FileError('两两比较方案须以（样本名/分组名_vs_样本名/分组名）的形式定义')
                elif len(re.findall(r'_vs_', vs)) > 1:
                    raise FileError('两两比较方案须以（样本名/分组名_vs_样本名/分组名）的形式定义且只能有一个_vs_')
                sams = vs.split('_vs_')
                if line.strip('\n').split()[1] not in sams:
                    raise FileError('两两比较方案中不含此对照组（样本）名字')
                vs_list.append(tuple(sams))
                if sams[0] == sams[1]:
                    raise FileError('两两比较方案中样本（分组）名字相同！')
            return num, vs_list

    def get_control_dict(self):
        """
        :return:键为两两比较的样本/分组的信息，值为对照样本/分组,实验样本/组
        """
        with open(self.prop['path'], 'rb') as tempfile:
            lines = tempfile.readlines()
            if not lines:
                raise FileError('对照组文件为空')
            adict = dict()
            for line in lines[1:]:
                info = line.strip().split()
                sam = info[0].split('_vs_')
                sam.remove(info[1].strip())
                adict[info[0]] = [info[1], sam[0]]
        return adict

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(ControlTableFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            self.get_info()
            return True
