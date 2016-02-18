# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""OTUtable格式文件类"""

from biocluster.iofile import File
import subprocess
from biocluster.config import Config
import os
import re
from biocluster.core.exceptions import FileError


class OtuTableFile(File):
    """
    OTUtable
    """
    def __init__(self):
        """
        """
        super(OtuTableFile, self).__init__()
        self.biom_path = os.path.join(Config().SOFTWARE_DIR, "Python/bin/")
        self.otu2shared_path = os.path.join(Config().SOFTWARE_DIR, "meta/scripts/otu2shared.pl")

    def get_info(self):
        """
        获取文件属性
        :return:
        """
        super(OtuTableFile, self).get_info()
        info = self.get_otuinfo()
        self.set_property("form", info[0])
        self.set_property("otu_num", info[1])
        self.set_property("sample_num", info[2])
        self.set_property("metadata", info[3])

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        if super(OtuTableFile, self).check():
            self.get_info()
            if self.prop['form']:
                pass
            else:
                raise FileError("文件格式错误")
        return True

    def get_otuinfo(self):
        """
        获取otu表信息
        """
        form, otu_num, sample_num, metadata = True, 0, 0, ''
        with open(self.prop['path'], 'r') as f:
            heads = f.readline().rstrip().split('\t')
            colnum = len(heads)
            # if not re.match(r'#*OTU ID', heads[0]):
            #     form = False
            if colnum < 2:
                form = False
            if form:
                sample_num = colnum - 1
                if heads[colnum - 1] == 'taxonomy':
                    metadata = 'taxonomy'
                    sample_num = colnum - 2
                while 1:
                    line = f.readline().rstrip()
                    otu_num += 1
                    if not line:
                        break
        return (form, otu_num, sample_num, metadata)

    def convert_to_biom(self, biom_filepath):
        """
        转换为biom格式
        """
        # biom convert -i otu_table.txt -o otu_table.biom.rev  --table-type "otu table  --to-hdf5"
        # biom convert -i otu_taxa_table.txt -o otu_table.biom.revtax  --table-type "otu table"  --to-hdf5 --process-obs-metadata taxonomy
        cmd = self.biom_path + "biom convert -i " + self.prop['path'] + " -o " + biom_filepath + ' --table-type \"OTU table\" --to-hdf5'
        if self.prop['metadata'] == "taxonomy":
            cmd += " --process-obs-metadata taxonomy"
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("biom convert 运行出错！")
        return True

    def convert_to_shared(self, shared_filepath):
        """
        转换为mothur的shared格式
        """
        # otu2shared.pl -i otutable -l 0.97 -o otu.shared
        if self.prop['metadata'] == "taxonomy":
            raise FileError(u"can not covert otutable with taxon info.")
        cmd = self.otu2shared_path + " -l 0.97 -i " + self.prop['path'] + " -o " + shared_filepath
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("otu2shared.pl 运行出错！")
        return True

    def del_taxonomy(self, otu_path, no_tax_otu_path):
        """
        删除taxonomy列  并返回一个字典
        """
        tax_dic = dict()
        with open(otu_path, 'rb') as r, open(no_tax_otu_path, 'wb') as w:
            line = r.next()
            line = line.rstrip('\n')
            line = re.split('\t', line)
            if line[-1] != "taxonomy":
                raise Exception("文件最后一列不为taxonomy，不需要去除taxonomy列")
            line.pop()
            w.write("\t".join(line) + "\n")
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                tax_dic[line[0]] = line[-1]
                line.pop()
                newline = "\t".join(line)
                w.write(newline + "\n")
        return tax_dic

    def add_taxonomy(self, tax_dic, no_tax_otu_path, otu_path):
        """
        根据字典， 为一个otu表添加taxonomy列
        """
        if not isinstance(tax_dic, dict):
            raise Exception("输入的tax_dic不是一个字典")
        with open(no_tax_otu_path, 'rb') as r, open(otu_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\t" + "taxonomy\n")
            line = re.split('\t', line)
            if line[-1] == "taxonomy":
                raise Exception("输入文件已经含有taxonomy列")
            for line in r:
                line = line.rstrip("\n")
                w.write(line)
                line = re.split("\t", line)
                w.write("\t" + tax_dic[line[0]] + "\n")

    def complete_taxonomy(self, otu_path, complete_path):
        """
        将一个OTU表的taxnomy列补全
        """
        with open(otu_path, 'rb') as r, open(complete_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\n")
            line = re.split('\t', line)
            if line[-1] != "taxonomy":
                raise Exception("文件taxonomy信息缺失，请调用complete_taxonomy_by_dic方法补全taxonomy")
            for line in r:
                line = line.rstrip("\n")
                line = re.split('\t', line)
                tax = line.pop()
                info = "\t".join(line)
                new_tax = self._comp_tax(tax)
                w.write(info + "\t" + new_tax + "\n")

    def complete_taxonomy_by_dic(self, tax_dic, otu_path, complete_path):
        """
        根据字典, 添加taxnomy列并补全
        """
        if not isinstance(tax_dic, dict):
            raise Exception("输入的tax_dic不是一个字典")
        with open(otu_path, 'rb') as r, open(complete_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\t" + "taxonomy\n")
            line = re.split('\t', line)
            if line[-1] == "taxonomy":
                raise Exception("输入文件已经含有taxonomy列, 请调用complete_taxonomy方法补全taxonomy")
            for line in r:
                line = line.rstrip("\n")
                w.write(line)
                line = re.split('\t', line)
                tax = tax_dic[line[0]]
                new_tax = self._comp_tax(tax)
                w.write("\t" + new_tax + "\n")

    @staticmethod
    def _comp_tax(tax):
        LEVEL = {
            0: "d__", 1: "k__", 2: "p__", 3: "c__", 4: "o__",
            5: "f__", 6: "g__", 7: "s__"
        }
        begin_index = 100
        last_info = ""
        tax = re.sub(r'\s', '', tax)
        cla = re.split(';', tax)
        #  处理uncultured和Incertae_Sedis
        i = 0
        for my_cla in cla:
            if re.search("uncultured", cla[i]) or re.search("Incertae_Sedis", cla[i]):
                if i == 0:
                    raise Exception("在域水平上的分类为uncultured或Incertae_Sedis")
                else:
                    cla[i] = cla[i] + "_" + cla[i - 1]
            i += 1
        for i in range(8):
            if not re.search(LEVEL[i], tax):
                begin_index = i  # 从哪个级别开始，缺失信息
                if i == 0:
                    raise Exception("在域水平缺失信息")
                last_info = cla[i - 1]
                break
        if begin_index < 8:
            for i in range(begin_index, 8):
                my_str = LEVEL[i] + "Unclasified_" + last_info
                cla.append(my_str)
        new_tax = "; ".join(cla)
        return new_tax
