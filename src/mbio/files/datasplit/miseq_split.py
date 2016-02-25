# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import simplejson as json
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
        self.seq_config = ["index_missmatch", "ignore_missing_bcl", "base_mask"]
        self.p_props = ["sample_name", "index", "sample_id", "mj_sn",
                        "cus_sample_name", "lane", "project", "has_child", "program"]
        self.c_props = ["sample_name", "sample_id", "mj_sn", "config", "primer", "index"]  # "cus_sample_name"暂时缺失
        self.c_config = ["index_miss", "primer_miss", "filter_min"]

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
        self.set_property("split_id", self.jobj['split_id'])
        self.set_property("sequcing_id", self.jobj['sequcing_id'])
        self.set_property("sequcing_sn", self.jobj['sequcing_sn'])
        self.set_property("split_id", self.jobj['split_id'])
        self.set_property("program", self.jobj['program'])
        self.set_property("file_path", self.jobj['file_path'])
        self.set_property("index_missmatch", int(self.jobj["config"]["index_missmatch"]))
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
            raise ValueError("父样本 %s 不存在子样本" % sample_id)
        for c_id in self.prop["child_ids"]:
            if self.child_sample(c_id, "sample_name") == self.parent_sample(sample_id, "sample_name"):
                id_list.append(self.child_sample(c_id, "sample_id"))
        return id_list

    def find_parent_id(self, c_id):
        """
        根据子样本的sample_id, 查找他的父样本的id
        """
        sample_name = self.child_sample(c_id, 'sample_name')
        for p in self.prop["parent_sample"]:
            if p["sample_name"] == sample_name:
                return p['sample_id']

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

    def check_parent_repeat(self):
        """
        检测一块下机板中父样本的index是否重复
        """
        p_index = list()
        for p in self.prop['parent_sample']:
            my_index = p["index"]
            if my_index in p_index:
                raise FileError("父样本中的index重复")
            else:
                p_index.append(my_index)
        return True

    def check_child_repeat(self):
        """
        检测同属于一个父样本的子样本的index是否重复
        """
        for p_id in self.prop['parent_ids']:
            try:
                c_ids = self.find_child_ids(p_id)
            except ValueError:
                c_ids = []
            c_index = list()
            for c_id in c_ids:
                my_index = self.child_sample(c_id, "index")
                if my_index in c_index:
                    raise FileError("属于同一个父样本的子样本中的index重复")
                else:
                    c_index.append(my_index)
        return True

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

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        """
        if super(MiseqSplitFile, self).check():
            self.get_info()
            if self.ckeck_prop() and self.check_child_repeat() and self.check_parent_repeat():
                return True
