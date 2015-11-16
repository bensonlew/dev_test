# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File, Directory
import os
from biocluster.core.exceptions import FileError


class BoxOutdirFile(Directory):
    """
    定义boxoutdir文件
    """

    def __init__(self):
        super(BoxOutdirFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(BoxOutdirFile, self).get_info()
        boxinfo = self.get_box_info()
        self.set_property('box_list', boxinfo)

    def get_box_info(self):
        """
        获取数据文件中每个箱子的名称

        :return box_list: 所有需要绘制的箱子名
        """
        tempfile = open(self.prop['path'].rstrip('/') + '/' + self.getfile())
        box_list = [i.split()[0] for i in tempfile]
        tempfile.close()
        return box_list

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        """
        if super(BoxOutdirFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            boxfile = self.getfile()
            box = File()
            box.set_path(self.prop['path'].rstrip('/') + '/' + box_file)
            box.check()

    def getfile(self):
        """
        获取并返回箱式图的数据文件名
        :return box_file: 箱式图数据文件名
        """
        filelist = os.listdir(self.prop['path'])
        box = 0
        box_file = ''
        for name in filelist:
            if '_Distances.txt' in name:
                box += 1
                box_file = name
        if box != 1:
            print 'BBBBBBBBBB',box,'AAAAAAAAAAAAAAAAAAA'
            raise FileError('不存在或者存在多个箱式图数据文件')
        else:
            return box_file
