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
        self.seq_prop = ["sequcing_id", "sequcing_sn", "program", "file_path",
                         "config", "parent_sample", "child_sample"]
        self.seq_config = ["index_mismatch", "ignore_missing_bcl", "base_mask"]
        self.p_props = ["sample_name", "index", "filter.min", "sample_id", "mj_sn",
                        "cus_sample_name", "lane", "project", "has_child", "program"]
        self.c_props = ["sample_name", "sample_id", "mj_sn", "cus_sample_name", "config"]
        self.c_config = ["index", "primer", "index_miss", "primer_miss", "filter.min"]

    def get_info(self):
        """
        获取文件属性
        "file_path": 测序版路径
        "parent_sample"：dict 全部的父样本信息
        "child_sample": dict 全部的子样本信息
        "parent_ids"：list 全部的父样本id
        "child_ids"：list 全部的子样本is
        "projects"： list 全部的project，涉及到bcl2fastq的拆分结果的目录结构
        """
        super(MiseqSplitFile, self).get_info()
        self.jobj = self.dump_json()
        for p in self.seq_prop:
            if p not in self.jobj:
                raise FileError("json中缺少属性：" + p)
        self.set_property("sequcing_id", self.jobj['file_path'])
        self.set_property("file_path", self.jobj['file_path'])
        self.set_property("index_mismatch", self.jobj["config"]["index_mismatch"])
        self.set_property("ignore_missing_bcl", self.jobj["config"]['ignore_missing_bcl'])
        self.set_property("base_mask", self.jobj["config"]["base_mask"])
        self.set_property("parent_sample", self.jobj["parent_sample"])
        self.set_property("child_sample", self.jobj["child_sample"])
        p_id_list = list()
        project_list = list()
        for p in self.prop["parent_sample"]:
            p_id_list.append(p["sample_id"])
            project_list.append(p["project"])
        c_id_list = list()
        for c in self.prop["child_sample"]:
            c_id_list.append(c["sample_id"])
        project_list = list(set(project_list))
        self.set_property("parent_ids", p_id_list)
        self.set_property("child_ids", c_id_list)
        self.set_property("projects", project_list)

    def dump_json(self):
        """
        解析json
        """
        with open(self.prop['path'], 'r') as r:
            str_ = r.read()
        try:
            jobj = json.loads(str_)
        except json.scanner.JSONDecodeError:
            raise FileError("Json格式不正确")
        return jobj

    def parent_sample(self, sample_id, prop):
        """
        获取父样本的一项属性值
        :param sample_id: 父样本id
        :param prop: 父样本的属性 必须是列表self.p_props的一个值
        """
        if self.prop["parent_sample"] == "":
            raise ValueError("不存在父样本！")
        if not self.has_parent_sample(sample_id):
            raise ValueError("不存在父样本 %s" % sample_id)
        if prop not in self.p_props:
            raise ValueError("父样本不存在属性值 %s" % prop)
        for p_sample in self.prop["parent_sample"]:
            if p_sample["sample_id"] == sample_id:
                return p_sample[prop]

    def child_sample(self, sample_id, prop):
        """
        获取子样本的一项属性
        :param sample_id: 子样本id
        :param prop: 子样本的属性，必须是列表c_props或者列表c_config里的一个值
        """
        if self.prop['child_sample'] == "":
            raise ValueError("不存在子样本！")
        if not self.has_child_sample(sample_id):
            raise ValueError("不存在子样本 %s" % sample_id)
        if (prop not in self.c_props) and (prop not in self.c_config):
            raise ValueError("子样本不存在属性值 %s" % prop)
        for c_sample in self.prop["child_sample"]:
            if c_sample["sample_id"] == sample_id:
                if prop in self.c_props:
                    return c_sample[prop]
                elif prop in self.c_config:
                    return c_sample["config"][prop]
                else:
                    raise ValueError("未知错误!")

    def find_child_ids(self, sample_id):
        """
        根据父样本的sample_id，查找他的所有子样本的id的集合
        """
        id_list = list()
        if not self.parent_sample(sample_id, "has_child"):
            raise ValueError("不存在子样本 %s" % sample_id)
        for c_id in self.prop["child_ids"]:
            if self.child_sample(c_id, "sample_name") == self.parent_sample(sample_id, "sample_name"):
                id_list.append(self.child_sample(c_id, "sample_id"))
        return id_list

    def has_child_sample(self, sample_id):
        """
        检查一块下机版中是否含有某样本名的子样本
        :param name: 子样本名称
        """
        for c_sample in self.prop["child_sample"]:
            if c_sample["sample_id"] == sample_id:
                return True
        return False

    def has_parent_sample(self, sample_id):
        """
        检查一块下机版中是否含有某样本名的父样本
        :param name: 父样本名称
        """
        for p_sample in self.prop["parent_sample"]:
            if p_sample["sample_id"] == sample_id:
                return True
        return False

    def ckeck_prop(self):
        for p in self.p_props:
            if p not in self.prop["parent_sample"][0].keys():
                raise FileError("Json文件中父样本样本属性%s缺失！" % p)
        for c in self.c_props:
            if c not in self.prop["child_sample"][0].keys():
                raise FileError("Json文件中子样本属性 %s 缺失！" % p)
        for c in self.c_config:
            if c not in self.prop["child_sample"][0]["config"]:
                raise FileError("Json文件中子样本config属性 %s 缺失！" % c)
        return True

    def check_config(self):
        for c in self.c_props:
            for c_sample in self.prop["child_sample"]:
                line = re.split('_', c_sample['config']["primer"])
                if len(line) != 2:
                    raise FileError("Json文件中子样本primer属性值错误")
        return True

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        """
        if super(MiseqSplitFile, self).check():
            if self.ckeck_prop() and self.check_config():
                return True

if __name__ == '__main__':
    a = MiseqSplitFile()
    a.set_path("example_json.json")
    a.get_info()
    a.check()
    print a.parent_sample("f0001", "project")
    print a.child_sample("c0004", "cus_sample_name")
    print a.child_sample("c0011", "index")
    print a.prop['child_ids']
