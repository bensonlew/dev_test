# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
from collections import defaultdict


class GroupEnv(Base):
    def __init__(self, bind_object):
        super(GroupEnv, self).__init__(bind_object)
        self._db_name = "sanger"
        self.scheme = list()
        self.info_dict = dict()

    @report_check
    def add_ini_group_table(self, file_path, table_type, task_id=None, name=None):
        if table_type == "group":
            self.collection = self.db['sg_specimen_group']
            if not name:
                name = "initial_group_table"
        if table_type == "env":
            self.collection = self.db['sg_env']
            if not name:
                name = "initial_env_table"
        if task_id is None:
            task_id = self.bind_object.sheet.id

        (self.info_dict, self.scheme) = self._get_table_info(file_path)
        for s in self.scheme:
            self._insert_one_table(s, self.info_dict, name, task_id, file_path, table_type)

    def _insert_one_table(self, s, name, task_id, file_path, table_type):
        category_names = list()
        specimen_names = list()
        for k in self.info_dict:
            if k[0] == s:
                category_names.append(k[1])
        for i in range(len(category_names)):
            for k in self.info_dict:
                if k[0] == s and k[1] == category_names[i]:
                    tmp_dic = dict()
                    for sp in self.info_dict[k]:
                        tmp_dic[sp] = sp
                    specimen_names.append(tmp_dic)
        insert_date = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": task_id,
            "name": name,
            "group_name": s,
            "category_names": category_names,
            "specimen_names": specimen_names
        }
        try:
            self.collection.insert_one(insert_date)
        except Exception as e:
                self.bind_object.logger.error("导入{}表格{}失败：{}".format(table_type, file_path, e))
        else:
            self.bind_object.logger.info("导入{}表格{}成功".format(table_type, file_path))

    def _get_table_info(self, file_path):
        info_dic = defaultdict(list)  # info_dict[(分组方案名, 组名)] = [样本1,  样本2,...]
        scheme = list()  # 分组方案
        index_gpname = dict()
        with open(file_path, 'rb') as r:
            line = r.next().rstrip("\r\n")
            line = re.split('\t', line)
            for i in range(1, len(line)):
                index_gpname[i] = line[i]
                scheme.append(line[i])
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                for i in range(1, len(line)):
                    info_dic[(index_gpname[i], line[i])].append(line[0])
        return (info_dic, scheme)
