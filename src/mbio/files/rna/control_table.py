# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError
from collections import Counter


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
        num, vs_list = self.get_control_info()
        if num == 0:
            raise Exception('对照方案至少为1')
        self.set_property('vs_list', vs_list)

    def get_control_info(self):
        """
        :return:对照样本（组）数目：num；包含两两比较的样本（分组）元组的列表:[(对照，实验), (对照，实验)]
        """
        with open(self.prop['path'], 'rb') as r:
            lines = r.readlines()
            if not lines:
                raise FileError('对照组文件为空')
            if not re.match(r'^#', lines[0]):
                raise FileError('对照文件格式有误，表头应为#开头')
            num = len(lines) - 1
            vs_list = []
            sam_list = []
            for line in lines[1:]:
                control = line.strip('\n').split()[0]
                other = line.strip('\n').split()[1]
                vs_list.append((control, other))
                sam_list.append((other, control))
                if control == other:
                    raise FileError('对照组：{}与实验组：{}名字相同！'.format(control, other))
            sam_list += vs_list
            count = Counter(sam_list).values()
            for i in count:
                if i != 1:
                    raise FileError("同一个两两比较分组中出现不同的对照组，请检查")
            return num, vs_list

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(ControlTableFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            self.get_info()
            return True
