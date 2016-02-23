# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
from collections import defaultdict


class Group(Base):
    def __init__(self, bind_object=None):
        super(Group, self).__init__(bind_object)
        self._db_name = "sanger"
        self.scheme = list()
        self.info_dict = dict()

    @report_check
    def add_ini_group_table(self, file_path, spname_spid, task_id=None):
        self.collection = self.db['sg_specimen_group']
        if task_id is None:
            task_id = self.bind_object.sheet.id

        (self.info_dict, self.scheme) = self._get_table_info(file_path, spname_spid)
        for s in self.scheme:
            self._insert_one_table(s, task_id, file_path)

    def _insert_one_table(self, s, task_id, file_path):
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
            # "project_sn": "test_project_sn",
            "task_id": task_id,
            "group_name": s,
            "category_names": category_names,
            "specimen_infos": specimen_names
        }
        try:
            self.collection.insert_one(insert_date)
        except Exception as e:
                self.bind_object.logger.error("导入{}表格{}失败：{}".format(file_path, e))
        else:
            self.bind_object.logger.info("导入{}表格{}成功".format(file_path))

    def _get_table_info(self, file_path, spname_spid):
        info_dic = defaultdict(list)  # info_dict[(分组方案名, 组名)] = [样本1_id,  样本2_id,...]
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
                    if line[0] not in spname_spid:
                        raise Exception("意外错误,样本名{}在以导入的样本当中未找到".format(line[0]))
                    info_dic[(index_gpname[i], line[i])].append(str(spname_spid(line[0])))
        return (info_dic, scheme)

if __name__ == "__main__":
    gp = Group()
    file_path = "test_group.txt"
    gp.add_ini_group_table(file_path, "group", "test", "test_group")
