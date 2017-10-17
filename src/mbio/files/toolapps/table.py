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
            # file_name = os.path.basename(self.prop['path'])
            # if file_name.split(".")[-1] != 'txt':
            #     raise FileError("文件类型不对,应该为TXT格式,其后缀应该为.txt")  # changed by wzy,20170926  注释掉
            f = open(self.prop['path'], 'r')
            code_dic = chardet.detect(f.read())
            if code_dic['encoding'] != 'ascii' and code_dic['encoding'] != 'UTF-16LE':
                raise FileError('文件编码格式不符合要求')
            new_path = self.get_newtable(code_dic['encoding'])
            self.set_property("new_table", new_path)
            self.check_info(new_path)  # 判断是否符合数据表格的要求
            self.set_property('sample_num', self.col_number)
        else:
            raise FileError("文件路径不正确，请设置正确的文件路径!")

    def get_newtable(self, encoding):
        dos2unix_path = Config().SOFTWARE_DIR + '/bioinfo/hd2u-1.0.0/bin/dos2unix'
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
                # os.system('dos2unix -c Mac {}'.format(self.prop['path']))  # 转换输入文件
                # os.system('{} -c Mac {}'.format(dos2unix_path, self.prop['path']))  # 转换输入文件
                subprocess.check_output(dos2unix_path + ' -C ' + self.prop['path'], shell=True) 
            else:
                subprocess.check_output(dos2unix_path + ' ' + self.prop['path'], shell=True)
                # os.system('{} {}'.format(dos2unix_path, self.prop['path']))  # 转换输入文件
                # os.system('dos2unix {}'.format(self.prop['path']))  # 转换输入文件
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
            first_line = f.readline().strip('\r\n').split('\t')
            f1 = set(first_line)
            if len(f1) != len(first_line):
                raise FileError('列名不能重复_{}'.format(first_line))
            for m in first_line[1:]:
                if re.match('^[a-zA-Z0-9_]+$', m):
                    continue
                else:
                    raise FileError('列名中只能含数字/字母/下划线_{}'.format(m))
            self.col_number = len(first_line)
            for i in first_line:
                if i.isdigit():
                    raise FileError('列名中不能存在数字_{}'.format(i))
                else:
                    continue
            row_name = []
            for line in f:
                content = line.strip('\r\n').split('\t')
                if content[0] in row_name:
                    raise FileError('行名不能重复_{}'.format(content[0]))
                else:
                    row_name.append(content[0])
                    if re.match('^[a-zA-Z0-9_]+$', content[0]):
                        pass
                    else:
                        raise FileError('行名中只能含数字/字母/下划线_{}'.format(content[0]))
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


if __name__ == "__main__":
    a = TableFile()
    a.set_path("/mnt/ilustre/users/sanger-dev/sg-users/zhouxuan/otu_table_Tab_zx_1498725491.txt")
    a.check()
