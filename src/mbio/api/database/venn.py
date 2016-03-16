# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
import os
import json
from collections import defaultdict
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from mbio.packages.meta.otu.export_otu import export_otu_table_by_level


class Venn(Base):
    def __init__(self, bind_object):
        super(Venn, self).__init__(bind_object)
        self._db_name = "sanger"
        self.new_otu_id = list()

    @report_check
    def add_venn_detail(self, venn_path, venn_id, otu_id, level):
        sub_otu_dir = os.path.join(self.bind_object.work_dir, "sub_otu")
        os.mkdir(sub_otu_dir)
        self._find_info(otu_id)
        self.level = level
        self.all_otu = os.path.join(sub_otu_dir, "all.otu")
        export_otu_table_by_level(otu_id, 9, self.all_otu)
        if not isinstance(venn_id, ObjectId):
            if isinstance(venn_id, StringTypes):
                venn_id = ObjectId(venn_id)
            else:
                raise Exception("venn_id必须为ObjectId对象或其对应的字符串!")
        venn_json = self.get_venn_json(venn_path)
        with open(venn_path, 'rb') as r:
            count = 0
            for line in r:
                line = line.strip('\n')
                line = re.split("\t", line)
                new_otu_id = self.add_sg_otu(otu_id, line[0])
                self.new_otu_id.append(new_otu_id)
                self.add_sg_otu_detail(line[2], otu_id, new_otu_id, line[0])
                tmp_name = re.split(',', line[2])
                name_list = list()
                for cla_info in tmp_name:
                    cla_info = re.split('; ', cla_info)
                    my_name = cla_info[-1]
                    name_list.append(my_name)
                display_name = ",".join(name_list)
                collection = self.db['sg_otu_venn_detail']
                insert_data = {
                    'otu_venn_id': venn_id,
                    'otu_id': new_otu_id,
                    'category_name': line[0],
                    'species_name': line[2],
                    'species_name_display': display_name,
                    'species_count': int(line[1]),
                    'venn_json': json.dumps(venn_json[count])
                }
                collection.insert_one(insert_data)
                count += 1

    def get_venn_json(self, venn_path):
        num = defaultdict(int)
        only = dict()
        gp = list()
        with open(self.bind_object.option("group_table").prop['path'], "rb") as r:
            r.next()
            for line in r:
                line = re.split('\t', line)
                if line[1] not in gp:
                    gp.append(line[1])
        gp_len = len(gp)
        sum_len = defaultdict(int)
        single_len = dict()
        # sum_len{分组的数目} : 该数目之下的和  例如该方案下有四个分组a,b,c,d 那么sum_len[3] 就应该是 abc， abd, bcd， acd 数目之和。
        # singel_len[(所包含的分组)]
        with open(venn_path, 'rb') as r:
            for line in r:
                line = line.strip('\r\n')
                line = re.split("\t", line)
                if re.search("only", line[0]):
                    name = re.split('\s+', line[0])
                    only[(name[0],)] = int(line[1])
                if re.search('&', line[0]):
                    line[0] = re.sub('\s+', '', line[0])
                    name = re.split('&', line[0])
                    single_len[tuple(name)] = int(line[1])
                    sum_len[len(name)] += int(line[1])
                    num[tuple(name)] += int(line[1])
        # print num
        # print only
        for my_only in only:
            num[my_only] = only[my_only]
            for i in range(2, gp_len + 1):
                num[my_only] += ((-1) ** i) * sum_len[i]
                for s_len in single_len:
                    if len(s_len) == i and my_only[0] not in s_len:
                        num[my_only] = num[my_only] + ((-1) ** (i + 1) * single_len[s_len])

        # 为了Venn图美观，平均化单个的大小， 对其他部分的大小进行缩小
        avg = 0
        c = 0
        # print num
        for name in num:
            if len(name) == 1:
                avg += num[name]
                c += 1
        avg = avg / c

        for name in num:
            for i in range(1, len(num) + 1):
                num[name] == avg / i

        venn_json = list()
        with open(venn_path, 'rb') as r:
            for line in r:
                line = line.strip('\n')
                strline = line
                line = re.split("\t", line)
                tmp_label = line[0]
                tmp_list = list()
                if re.search("only", line[0]):
                    name = re.split('\s+', line[0])[0]
                    """
                    sets = {"sets": [name]}
                    size = {"size": num[name]}
                    label = {"label": tmp_label}
                    tmp_list = [sets, size, label]
                    """
                    tmp_list = {"sets": [name], "size": num[(name,)], "label": tmp_label}
                elif re.search("&", line[0]):
                    line[0] = re.sub('\s+', '', line[0])
                    name = re.split('&', line[0])
                    """
                    sets = {'sets': name}
                    size = {"size": num[tuple(name)]}
                    label = {"label": tmp_label}
                    tmp_list = [sets, size, label]
                    """
                    tmp_list = {"sets": name, "size": num[tuple(name)], "label": tmp_label}
                else:
                    raise Exception("Venn 表格中行{}无法解析".format(strline))
                venn_json.append(tmp_list)
        return venn_json

    def add_sg_otu_detail(self, info, from_otu_id, new_otu_id, title):
        """
        对otu表进行筛选，当它符合venn_table里的结果时，将他输出到sub_otu_path中，形成一张otu子表，读取子表
        删除值全部为0的列，形成no_zero_path中的otu表，最后将这张表导入数据库中(sg_otu_detail, sg_speciem)
        """
        title = re.sub(r'\s', '', title)
        sub_otu_dir = os.path.join(self.bind_object.work_dir, "sub_otu")
        sub_otu_path = os.path.join(sub_otu_dir, title + ".sub")
        no_zero_path = os.path.join(sub_otu_dir, title + ".no_zero")
        selected_clas = re.split(',', info)
        o_otu_path = self.all_otu
        sample_num = defaultdict(int)
        level = self.bind_object.option("level")
        with open(o_otu_path, 'rb') as r, open(sub_otu_path, 'wb') as w:
            head = r.next()
            w.write(head)
            head = head.strip('\n\r')
            head = re.split('\t', head)
            for line in r:
                line = line.strip('\r\n')
                line = re.split('\t', line)
                full_classify = re.split("; ", line[0])
                venn_classsify = full_classify[0:level]
                str_ = "; ".join(venn_classsify)
                if str_ in selected_clas:
                    new_line = "\t".join(line)
                    w.write(new_line + "\n")
                    for i in range(1, len(line)):
                        sample_num[i] += int(line[i])
        # print sample_num
        new_head = self.del_zero_column(sub_otu_path, no_zero_path, sample_num, head)
        self.table_to_sg_otu_detail(no_zero_path, new_otu_id)
        self.add_sg_otu_specimen(from_otu_id, new_otu_id, new_head)

    def del_zero_column(self, sub_otu_path, no_zero_path, sample_num, head):
        index_list = list()
        new_head = list()  # 也就是样本名的列表（除去了head[0]和为0的列head）
        for i in sample_num:
            if sample_num[i]:
                index_list.append(i)
                new_head.append(head[i])
        with open(sub_otu_path, 'rb') as r, open(no_zero_path, 'wb') as w:
            w.write("OTU ID\t" + "\t".join(new_head) + "\n")
            line = r.next()
            for line in r:
                line = line.strip('\n\r')
                line = re.split('\t', line)
                w.write(line[0] + '\t')
                tmp_line = list()
                for i in index_list:
                    tmp_line.append(line[i])
                w.write('\t'.join(tmp_line) + "\n")
        return new_head

    def table_to_sg_otu_detail(self, no_zero_path, new_otu_id):
        data_list = list()
        with open(no_zero_path, 'rb') as r:
            line = r.next().strip('\r\n')
            head = re.split('\t', line)
            for line in r:
                insert_data = dict()
                line = line.strip('\r\n')
                line = re.split('\t', line)
                classify_list = re.split(r"\s*;\s*", line[0])
                for c in classify_list:
                    insert_data[c[0:3]] = c
                insert_data["otu_id"] = new_otu_id
                insert_data["task_id"] = self.task_id
                for i in range(1, len(line)):
                    insert_data[head[i]] = line[i]
                data_list.append(insert_data)
        collection = self.db['sg_otu_detail']
        collection.insert_many(data_list)

    def add_sg_otu(self, otu_id, name):
        if not isinstance(otu_id, ObjectId):
            otu_id = ObjectId(otu_id)
        if re.search(r'only', name):
            name = re.sub(' ', "_", name)
        if re.search(r'&', name):
            name = re.sub(' ', '', name)
        insert_data = {
            "project_sn": self.project_sn,
            "task_id": self.task_id,
            "from_id": otu_id,
            "name": "venn_otu_" + name + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "status": "end",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db['sg_otu']
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def _find_info(self, otu_id):
        if not isinstance(otu_id, ObjectId):
            otu_id = ObjectId(otu_id)
        collection = self.db['sg_otu']
        result = collection.find_one({'_id': otu_id})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_otu表里找到相应的记录".format(str(otu_id)))
        self.project_sn = result['project_sn']
        self.task_id = result['task_id']

    def add_sg_otu_specimen(self, otu_id, new_otu_id, samples):
        """
        添加sg_otu_specimen记录
        """
        matched_sample_id = list()
        data_list = list()
        collection = self.db['sg_otu_specimen']
        if not isinstance(otu_id, ObjectId):
            otu_id = ObjectId(otu_id)
        results = collection.find({"otu_id": otu_id})
        for result in results:
            my_collection = self.db["sg_specimen"]
            try:
                my_result = my_collection.find_one({"_id": result['specimen_id']})
            except:
                raise Exception("样本id:{}在sg_specimen表里未找到对应的记录".format(result['specimen_id']))
            if my_result["specimen_name"] in samples:
                matched_sample_id.append(result['specimen_id'])
        # print samples
        for m_id in matched_sample_id:
            insert_data = {
                "otu_id": new_otu_id,
                "specimen_id": m_id
            }
            data_list.append(insert_data)
        # print data_list
        collection.insert_many(data_list)
