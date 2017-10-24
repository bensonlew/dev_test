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
        self.unicode = False

        # 写一些需要用到的数据的定义

    def get_info(self):
        """
            获取文件的基本属性
            """
        super(SampleBaseTableFile, self).get_info()
        f = open(self.prop['path'], 'r')
        code_dic = chardet.detect(f.read())
        if code_dic['encoding'] != 'ascii' and code_dic['encoding'] != 'UTF-16LE' and code_dic['encoding'] != 'GB2312':
            raise FileError('文件编码格式不符合要求')
        new_path = self.get_newtable(code_dic['encoding'])
        self.set_property("new_table", new_path)

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
                subprocess.check_output(dos2unix_path + ' -c Mac ' + self.prop['path'], shell=True)
            else:
                subprocess.check_output(dos2unix_path + ' ' + self.prop['path'], shell=True)
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
                        new.write('\t'.join(new_line) + '\n')
                return new_file_path
        elif encoding == 'GB2312':
            fp2 = open(self.prop['path'], 'r')
            with open(new_file_path, 'w') as new:
                lines = fp2.readlines()
                for line in lines:
                    tmp = line.strip().split("\t")
                    new_tmp = []
                    for i in tmp:
                        new_i = i.decode("GB2312").encode("utf-8")
                        new_tmp.append(new_i)
                    new_line = ('\t'.join(new_tmp) + '\n')
                    new.write(new_line)
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
                    for content in line:
                        line_c.append(str(content))
                    new.write('\t'.join(line_c) + '\n')
            return new_file_path

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


