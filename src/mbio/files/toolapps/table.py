# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
from biocluster.iofile import File
from biocluster.core.exceptions import FileError
from biocluster.config import Config
import os
import codecs
import chardet
import random


class TableFile(File):
    """
    定义小工具二维表格
    """
    def __init__(self):
        super(TableFile, self).__init__()
        self.unicode = False
        self.col_number = 0
        # 写一些需要用到的数据的定义

    def get_info(self):
        if 'path' in self.prop.keys():
            file_name = os.path.basename(self.prop['path'])
            if file_name.split(".")[-1] != 'txt':
                raise FileError("文件类型不对,应该为TXT格式,其后缀应该为.txt")
            f = open(self.prop['path'], 'r')
            code_dic = chardet.detect(f.read())
            if code_dic['encoding'] != 'ascii' and code_dic['encoding'] != 'UTF-16LE':
                raise FileError('文件编码格式不符合要求')
            self.set_property("new_table", self.get_newtable(code_dic['encoding']))
            self.check_info(self.get_newtable(code_dic['encoding']))  # 判断是否符合数据表格的要求
            self.set_property('sample_num', self.col_number)
        else:
            raise FileError("文件路径不正确，请设置正确的文件路径!")

    def get_newtable(self, encoding):
        dir_path = Config().WORK_DIR + '/tmp/convert_table'
        if os.path.exists(dir_path):
            pass
        else:
            os.mkdir(dir_path)
        new_file_path = os.path.join(dir_path, "{}_{}.txt".format(os.path.basename(self.prop['path']),
                                                                                   random.randint(1000, 10000)))
        if encoding == 'ascii':
            with open(self.prop['path'], 'r') as old:
                line = old.readlines()
            if len(line) == 1:
                os.system('dos2unix -c Mac {}'.format(self.prop['path']))  # 转换输入文件
            else:
                os.system('dos2unix {}'.format(self.prop['path']))  # 转换输入文件
            with open(self.prop['path'], 'r') as old:
                first_line = old.readline().strip("\n").split("\t")
            if len(first_line) > 1:
                return self.prop['path']
            else:
                with open(self.prop['path'], 'r') as old, open(new_file_path, 'w') as new:
                    for line in old:
                        line = line.strip('\n').split(' ')
                        new_line = []
                        for i in line:
                            if i != '':
                                new_line.append(i)
                            else:
                                continue
                        new.write(('\t').join(new_line) + '\n')
                return new_file_path
        else:
            fp2 = codecs.open(self.prop['path'], 'r', 'utf-16-le')
            lineList = fp2.readlines()
            with open(new_file_path, 'w') as new:
                n = 0
                for line in lineList:
                    n += 1
                    line = line.strip('\r\n').split('\t')
                    line_c = []
                    if n == 1:
                        line = line[1:]
                    for content in line:
                        line_c.append(str(content))
                    if n == 1:
                        new.write("#sample\t" + ('\t').join(line_c) + '\n')
                    else:
                        new.write(('\t').join(line_c) + '\n')
            return new_file_path

    # @staticmethod
    def check_info(self, file_path):
        with open(file_path, 'r') as f:
            first_line = f.readline().strip('\n').split('\t')
            self.col_number = len(first_line)
            for i in first_line:
                if i.isdigit():
                    raise FileError('列名中不能存在数字')
                else:
                    continue
            for line in f:
                content = line.strip('\n').split('\t')
                if len(content) != self.col_number:
                    raise FileError('该表格行列信息不全——{}'.format(content))
                if content[0].isdigit():
                    raise FileError('行名中不能存在数字')
                for i in content[1:]:
                    if float(i) or i == '0':
                        continue
                    else:
                        raise FileError('二维数据表格内容必须为数字_{}'.format(i))

    def check(self):
        if super(TableFile, self).check():
            self.get_info()
            return True