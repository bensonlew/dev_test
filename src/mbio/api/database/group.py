# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# lastmodied = 'shenghe'  # 重构的导入方式

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


class Group(Base):
    def __init__(self, bind_object=None):
        super(Group, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_ini_group_table(self, file_path, spname_spid, task_id=None):
        self.collection = self.db['sg_specimen_group']
        # 解析文件
        with open(file_path) as f:
            names = f.readline().strip()
            names = names.split('\t')[1:]
            groups_dict = [OrderedDict() for i in names]
            for i in f:
                groups = i.strip().split('\t')
                sample = groups[0]
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
