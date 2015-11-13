# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory


class ReadsLenInfoDirFile(Directory):
    """
    定义reads_len_info文件夹格式
    """
    def __init__(self):
        """
        :param reads_len_info: 不带路径的reads_len_info文件名集合
        """
        super(ReadsLenInfoDirFile, self).__init__()
        self.reads_len_info = list()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            self.set_file_number(4)
        else:
            raise FileError(u"文件夹路径不正确，请设置正确的文件夹路径!")

    def get_reads_len_info_number(self):
        """
        获取文件夹下的reads_len_info文件的数目
        :return:文件数目
        """
        filelist = os.listdir(self.prop['path'])
        count = 0
        for file_ in filelist:
            if re.search(r'\.reads_len_info$', file_):
                count += 1
                self.reads_len_info.append(file_)
        return count

    def _check_file_name(self):
        """
        检查reads_len_info的文件名是否符合要求
        """
        list_ = ["1", "20", "50", "100"]
        for file_ in self.reads_len_info:
            (str_, step) = re.split(r'-', file_)
            if str_ != "step" or step not in list_:
                raise FileError(file_ + '命名不符合规范')

    def check(self):
        """
        检测文件夹是否满足要求，不满足时触发FileError异常
        """
        if super(ReadsLenInfoDirFile, self).check():
            if self.prop['file_number'] != self.get_reads_len_info_number():
                raise FileError("文件夹里reads_len_info文件数目不为四个！")
            self._check_file_name()
