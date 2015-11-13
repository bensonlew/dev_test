# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""OTUtable格式文件类"""

from biocluster.iofile import File
import re
import subprocess
from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class OtuTableFile(File):
    """
    OTUtable
    """
    def __init__(self):
        """
        """
        super(OtuTableFile, self).__init__()
        self.biom_path = os.path.join(Config().SOFTWARE_DIR, "/Python/bin")
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
        self.set_property("sample_num", info[1])
        self.set_property("metadata", info[2])

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        if super(OtuTableFile, self).check():
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
            if not re.match(r'#*OTU ID', heads[0]):
                form = False
            if colnum < 2:
                form = False
            if form:
                sample_num = colnum-1
                if heads[colnum-1] == 'taxonomy':
                    metadata = 'taxonomy'
                    sample_num = colnum-2
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
        cmd = self.biom_path+"biom convert -i " + self.prop['path'] + " -o " + biom_filepath + ' --table-type \"OTU table\" --to-hdf5'
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
            raise FileError("can not covert otutable with taxon info.")
            os.sys.exit()
        cmd = self.otu2shared_path + " -i " + self.prop['path'] + " -o " + shared_filepath
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("otu2shared.pl 运行出错！")
        return True

