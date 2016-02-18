# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class GroupTableFile(File):
    """
    定义group_table格式文件
    """
    def __init__(self):
        super(GroupTableFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(GroupTableFile, self).get_info()
        info = self.get_file_info()
        self.set_property("sample_number", len(info[0]))
        self.set_property("sample", info[0])
        self.set_property("group_scheme", info[1])

    def get_file_info(self):
        """
        获取group_table文件的信息
        """
        row = 0
        self.format_check()
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            line = f.readline().rstrip("\r\n")
            line = re.split("\t", line)
            header = list()
            len_ = len(line)
            for i in range(1, len_):
                header.append(line[i])
            for line in f:
                line = line.rstrip("\r\n")
                line = re.split("\t", line)
                row += 1
                if line[0] not in sample.keys():
                    sample[line[0]] = 1
            return (sample, header)

    def format_check(self):
        with open(self.prop['path'], 'r') as f:
            line = f.readline().rstrip("\r\n")
            if not re.search("^#", line[0]):
                raise FileError("该group文件不含表头，group表第一列应该以#号开头")
            line = line.split("\t")
            length = len(line)
            if length < 2:
                raise FileError('group_table 文件至少应该有两列')
            for line in f:
                line = line.rstrip("\r\n")
                line = re.split("\t", line)
                for l in line:
                    if re.search("\s", l):
                        raise FileError('分组名里不可以包含空格')
                len_ = len(line)
                if len_ != length:
                    raise FileError("文件的列数不相等")

    def check(self):
        if super(GroupTableFile, self).check():
            self.get_info()
            if self.prop['sample_number'] == 0:
                raise FileError('应该至少包含一个样本')

    def sub_group(self, target_path, header):
        """
        :param target_path:  生成的子group表的位置
        :param header: 需要提取的子分组方案名，列表
        """
        if not isinstance(header, list):
            raise Exception("第二个参数的格式错误， 应该是一个python的列表")
        my_index = list()
        for h in header:
            if h not in self.prop['group_scheme']:
                raise Exception("{}不存在该表的分组方案中".format(h))
        len_ = len(self.prop['group_scheme'])
        for i in range(0, len_):
            if self.prop['group_scheme'][i] in header:
                my_index.append(i + 1)
        with open(self.prop['path'], 'r') as f, open(target_path, 'w') as w:
            line = f.readline().rstrip("\r\n")
            line = re.split("\t", line)
            new_header = list()
            for i in my_index:
                new_header.append(line[i])
            w.write("#sample\t{}\n".format("\t".join(new_header)))
            for line in f:
                sub_line = list()
                line = line.rstrip("\r\n")
                line = re.split("\t", line)
                sub_line.append(line[0])
                for i in my_index:
                    sub_line.append(line[i])
                new_line = "\t".join(sub_line)
                w.write(new_line + "\n")

if __name__ == "__main__":
    g = GroupTableFile()
    g.set_path("example.group")
    g.get_info()
    g.sub_group("example.group.sub", ["g1", "g3", "g4"])
