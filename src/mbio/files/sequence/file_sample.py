# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class FileSampleFile(File):
    """
    定义 文件名——样本 的文件格式
    这里的名称是文件名，当qc模块的输入是一个fastq文件夹的时候，用于规范化文件名
    """
    def __init__(self):
        super(FileSampleFile, self).__init__()
        self.col = 0
        self.repeat_name = False
        self.se_repeat = False      # modify by qiuping 2016.07.22,add 2 line
        self.pe_repeat = False
        self.file_sample = dict()  # 文件名与样本名的对应

    def get_info(self):
        """
        获取文件属性
        """
        super(FileSampleFile, self).get_info()
        (sample, name) = self.get_file_info()
        self.set_property("sample_number", len(sample))
        self.set_property("file_number", len(name))
        self.set_property("file_names", name.keys())
        self.set_property("sample_names", sample.keys())
        self.set_property("file_sample", self.file_sample)

    def get_file_info(self):
        """
        获取file_sample文件的信息
        """
        dir_name = os.path.dirname(self.prop['path'])
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            name = dict()
            for line in f:
                if "#" in line:
                    continue
                line = line.rstrip('\n')
                line = line.split()
                self.col = len(line)
                if self.col != 2 and self.col != 3:   # modify by qiuping 2016.07.22
                    raise FileError('这个文件的列数应该为2(SE)或者3(PE)')
                if line[1] not in sample.keys():
                    sample[line[1]] = 1
                else:                   # modify by qiuping 2016.07.22,add 2 lines
                    sample[line[1]] += 1
                if line[0] not in name.keys():
                    name[line[0]] = 1
                else:
                    self.repeat_name = True
                full_name = os.path.join(dir_name, line[0])
                if os.path.isfile(full_name):
                    self.file_sample[line[0]] = line[1]
                if self.col == 3:      # modify by qiuping 2016.07.22,add 3 lines
                    if line[2] not in ['l', 'r']:
                        raise FileError('为PE测序时，标识左右两端的字段不符合要求：l,r')
        return sample, name

    def get_list(self):
        """
        return::
           SE时，返回一个字典，样本重复：{'样本名'：'fq文件名，fq文件名'}，否则{'样本名'：'fq文件名'}
           PE时，返回一个字典，样本重复：{'样本名'：{'r'：'fq文件名，fq文件名','l'：'fq文件名，fq文件名'}}，否则{'样本名'：{'r'：'fq文件名'，'l'：'fq文件名'}}
        """
        file_sample = {}
        with open(self.prop['path'], "rb") as l:
            for line in l:
                line = line.strip().split()
                if len(line) == 3:
                    if line[1] not in file_sample:
                        if line[2] == 'r':
                            file_sample[line[1]] = {"r": line[0]}
                        else:
                            file_sample[line[1]] = {"l": line[0]}
                    else:
                        if line[2] == 'r':
                            if 'r' in file_sample[line[1]]:
                                file_sample[line[1]]['r'] += ',{}'.format(line[0])
                            else:
                                file_sample[line[1]]['r'] = line[0]
                        else:
                            if 'l' in file_sample[line[1]]:
                                file_sample[line[1]]['l'] += ',{}'.format(line[0])
                            else:
                                file_sample[line[1]]['l'] = line[0]
                if len(line) == 2:
                    if line[1] not in file_sample:
                        file_sample[line[1]] = line[0]
                    else:
                        file_sample[line[1]] += ',{}'.format(line[0])
        return file_sample

    def check_exists(self):
        """
        检查file_list中的每个文件是否都存在
        """
        dir_name = os.path.dirname(self.prop['path'])
        with open(self.prop['path'], 'r') as f:
            for line in f:
                if "#" in line:
                    continue
                line = line.rstrip().split()
                full_name = os.path.join(dir_name, line[0])
                if not os.path.isfile(full_name):
                    raise FileError("文件{}不存在".format(full_name))
        return True

    def check(self):
        if super(FileSampleFile, self).check():
            self.get_info()
            if not os.path.exists(self.prop['path']):
                raise FileError('文件路径不存在，请检查')
            if self.prop["sample_number"] == 0:
                raise FileError('应该至少包含一个样本')
            if self.repeat_name:
                raise FileError('文件名不能重复！')
            # modify by qiuping 2016.07.22,add 10 lines
            (sample, name) = self.get_file_info()
            for num in sample.values():
                if self.col == 3:
                    if num < 2:
                        raise FileError("PE测序时一个样本至少要有两个fastq文件")
                    if num > 2:
                        self.pe_repeat = True
                if self.col == 2:
                    if num >= 2:
                        self.se_repeat = True
            self.check_exists()
            return True
