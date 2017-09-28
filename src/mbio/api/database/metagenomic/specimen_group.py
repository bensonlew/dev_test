# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# lastmodied = 'shenghe'  # 重构的导入方式

from bson import ObjectId
from biocluster.api.database.base import Base, report_check
from collections import OrderedDict
from biocluster.config import Config
"""
分组方案格式如下:
{
    "group_name" : "G6",
    "category_names" : [
        "Vad_s",
        "Vad_d",
    ],
    "specimen_names" : [
        {
            "58b4eca103eeb1542dcae5e1" : "2",
            "58b4eca103eeb1542dcae5e3" : "4",
            "58b4eca103eeb1542dcae5e9" : "12",
            "58b4eca103eeb1542dcae5ef" : "18",
            "58b4eca103eeb1542dcae5db" : "20"
        },
        {
            "58b4eca103eeb1542dcae5e0" : "3",
            "58b4eca103eeb1542dcae5e2" : "5",
            "58b4eca103eeb1542dcae5e8" : "11",
            "58b4eca103eeb1542dcae5ee" : "19",
            "58b4eca103eeb1542dcae5dc" : "21"
        },
    ],
    "task_id" : "sanger_10389",
}
"""


class SpecimenGroup(Base):
    def __init__(self, bind_object=None):
        super(SpecimenGroup, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_ini_group_table(self, file_path, spname_spid, task_id=None, sort_samples=False):
        self.collection = self.db['specimen_group']
        # 解析文件
        with open(file_path) as f:
            names = f.readline().strip()
            names = names.split('\t')[1:]  # 分组方案名称
            groups_dict = [OrderedDict() for i in names]  # 各分组方案具体内容，元素为每个分组方案的有序字典
            samples = []  # 样本名列表
            for i in f:
                groups = i.strip().split('\t')
                sample = groups[0]
                samples.append(sample)
                for index, v in enumerate(groups[1:]):
                    if v in groups_dict[index]:
                        groups_dict[index][v].append(sample)
                    else:
                        groups_dict[index][v] = [sample]
        # 组合样本id
        for group in groups_dict:
            for category in group:
                id_name = []
                for i in group[category]:
                    if i not in spname_spid:
                        raise Exception('分组文件中的样本在提供的序列/样本中没有找到')
                    else:
                        id_name.append((str(spname_spid[i]), i))
                group[category] = OrderedDict(id_name)
        # 导入数据
        for index, name in enumerate(names):
            self.insert_one_group(name, groups_dict[index])
        self.bind_object.logger.info('分组文件中的所有分组方案导入完成。')
        if sort_samples:
            self.bind_object.logger.info("添加分组的样本顺序到样本表中")
            self.update_num_sgotuspecimen(spname_spid, samples)

    def update_num_sgotuspecimen(self, spname_spid, samples):
        """
        更新otu_specimen表加入一组编号字段，用于按照本分组文件排序样本。
        :params spname_spid: 样本名对样本id的字典
        :params samples: 样本列表
        """
        otu_specimen = self.db.otu_specimen
        for index, name in enumerate(samples):
            result = otu_specimen.update_many({'specimen_id': ObjectId(spname_spid[name])},
                                                 {"$set": {'order_num': index}})
            if result.matched_count < 1:
                raise Exception('没有正确将样本分组中的样本顺序更新到mongo数据表中')
        self.bind_object.logger.info('样本顺序信息更新到数据库中完成。')

    def insert_one_group(self, group_name, group):
        data = {
            'project_sn': self.bind_object.sheet.project_sn,
            'task_id': self.bind_object.sheet.id,
            'group_name': group_name,
            'category_names': group.keys(),
            'specimen_names': group.values()
        }
        self.collection.insert_one(data)
        self.bind_object.logger.info('导入样本分组方案{}成功'.format(group_name))
