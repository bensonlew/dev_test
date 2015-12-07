# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import simplejson as json
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class MiseqSplitFile(File):
    """
    定义miseq输入json文件的格式
    """
    def __init__(self):
        super(MiseqSplitFile, self).__init__()
        self.p_props = ["sample_name", "barcode", "filter.min"]
        self.c_props = ["sample_name", "barcode", "var_base", "parent_name", "primer", "barcode_miss", "primer_miss", "filter.min"]

    def get_info(self):
        """
        获取文件属性
        """
        super(MiseqSplitFile, self).get_info()
        self.jobj = self.dump_json()
        self.set_property("file_path", self.jobj['file_path'])
        self.set_property("barcode_mismatch", self.jobj['barcode_mismatch'])
        self.set_property("ignore_missing_bcl", self.jobj['ignore_missing_bcl'])
        self.set_property("base_mask", self.jobj["base_mask"])
        self.set_property("parent_sample", self.jobj["parent_sample"])
        self.set_property("child_sample", self.jobj["child_sample"])

    def dump_json(self):
        with open(self.prop['path'], 'r') as r:
            str_ = r.read()
        try:
            jobj = json.loads(str_)
        except json.scanner.JSONDecodeError:
            raise FileError("Json格式不正确")
        return jobj

    def parent_sample(self, name, prop):
        """
        获取父样本的一项属性值
        :param name: 父样本名称
        :param prop: 父样本的属性 必须是"sample_name", "barcode", "filter.min"当中的一个
        """
        if self.prop["parent_sample"] == "":
            raise ValueError("不存在父样本！")
        if not self.has_parent_sample(name):
            raise ValueError("不存在父样本 %s" % name)
        if prop not in self.p_props:
            raise ValueError("父样本不存在属性值 %s" % prop)
        for p_sample in self.prop["parent_sample"]:
            if p_sample["sample_name"] == name:
                return p_sample[prop]

    def child_sample(self, name, prop):
        """
        获取子样本的一项属性
        :param name: 子样本名称
        :param prop: 子样本的属性，必须是数组c_props里的一个值
        """
        if self.prop['child_sample'] == "":
            raise ValueError("不存在子样本！")
        if not self.has_child_sample(name):
            raise ValueError("不存在子样本 %s" % name)
        if prop not in self.c_props:
            raise ValueError("子样本不存在属性值 %s" % prop)
        for c_sample in self.prop["child_sample"]:
            if c_sample["sample_name"] == name:
                return c_sample[prop]

    def has_child_sample(self, name):
        """
        检查一块下机版中是否含有某样本名的子样本
        :param name: 子样本名称
        """
        for c_sample in self.prop["chile_sample"]:
            if c_sample["sample_name"] == name:
                return True
        return False

    def has_parent_sample(self, name):
        """
        检查一块下机版中是否含有某样本名的父样本
        :param name: 父样本名称
        """
        for p_sample in self.prop["parent_sample"]:
            if p_sample["sample_name"] == name:
                return True
        return False

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        """
        if super(MiseqSplitFile, self).check():
            for p in self.p_props:
                if p not in self.prop["parent_sample"][0].keys():
                    raise FileError("Json文件中父样本样本属性%s缺失！" % p)
            for c in self.c_props:
                if c not in self.prop["child_sample"][0].keys():
                    raise FileError("Json文件中子样本属性 %s 缺失！" % p)
                for c in self.prop["child_sample"]:
                    line = c["var_base"]
                    if len(line) != 2:
                        raise FileError("Json文件中子样本var_base属性值错误")
                    line = re.split("_", c["barcode"])
                    if len(line) != 2:
                        raise FileError("Json文件中子样本barcode属性值错误")
                    line = re.split('_', c["primer"])
                    if len(line) != 2:
                        raise FileError("Json文件中子样本primer属性值错误")

            return True

if __name__ == '__main__':
    a = MiseqSplitFile()
    a.set_path("example_json.json")
    a.check()
    print a.prop
