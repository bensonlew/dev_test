# -*- coding: utf-8 -*-
# __author__ = 'hongdongxuan'
# time: 2017.04.20


from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class PpiFile(File):
    """
    ppi文件夹格式
    """
    def __init__(self):
        super(PpiFile, self).__init__()

    def check(self):
        super(PpiFile, self).check()
        with open(self.prop["path"], "r") as r:
            line = r.readlines()[0]
            line = line.strip().split("\t")
            if str(line[0]) != "gene_id":
                raise FileError("基因列表中第一行第一个字段必须为gene_id")
            return True

# if __name__ == "__main__":
#     a = PpiFile()
#     a.set_path("/mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/test/9606.txt")
#     a.check()

