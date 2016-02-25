# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
import json
from collections import defaultdict
import datetime
from bson.objectid import ObjectId
from types import StringTypes


class Venn(Base):
    def __init__(self, bind_object):
        super(Venn, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_venn_detail(self, venn_path, venn_id, otu_id, level):
        self._find_info(otu_id)
        self.level = level
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
                new_otu_id = self.add_sg_otu(otu_id)
                self.add_sg_otu_detail(line[2], otu_id, new_otu_id)
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
        with open(venn_path, 'rb') as r:
            for line in r:
                line = line.strip('\n')
                line = re.split("\t", line)
                if re.search("only", line[0]):
                    name = re.split('\s+', line[0])
                    num[name[0]] += int(line[1])
                if re.search('&', line[0]):
                    line[0] = re.sub('\s+', '', line[0])
                    name = re.split('&', line[0])
                    for my_n in name:
                        num[my_n] += int(line[1])
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
                    sets = {"sets": [name]}
                    size = {"size": num[name]}
                    label = {"label": tmp_label}
                    tmp_list = [sets, size, label]
                elif re.search("&", line[0]):
                    line[0] = re.sub('\s+', '', line[0])
                    name = re.split('&', line[0])
                    sets = {'sets': name}
                    size = {"size": line[1]}
                    label = {"label": tmp_label}
                    tmp_list = [sets, size, label]
                else:
                    raise Exception("Venn 表格中行{}无法解析".format(strline))
                venn_json.append(tmp_list)
        return venn_json

    def add_sg_otu_detail(self, info, from_otu_id, new_otu_id):
        """
        对sg_otu_detail表里的记录进行筛选，当它符合venn_table里的结果时，将这条记录复制，并更改其otu_id(想到与对原otu表进行筛选， 形成一张新的otu表)
        """
        selected_clas = re.split(',', info)
        collection = self.db['sg_otu_detail']
        if not isinstance(from_otu_id, ObjectId):
            if isinstance(from_otu_id, StringTypes):
                try:
                    from_otu_id = ObjectId(from_otu_id)
                except:
                    raise Exception("传入的otu_id:{}必须为ObjectId对象或其对应的字符串!".format(from_otu_id))
            else:
                raise Exception("传入的otu_id:{}必须为ObjectId对象或其对应的字符串!".format(from_otu_id))
        results = collection.find({'otu_id': from_otu_id})
        if not results.count():
            raise Exception("传入的otu_id:{}在sg_otu_detail表里未找到具体记录".format(from_otu_id))
        for result in results:
            new_id = self._check_id(result, selected_clas)
            if new_id:
                result['task_id'] = self.task_id
                result['otu_id'] = new_otu_id
                del result['_id']
                collection.insert_one(result)

    def _check_id(self, result, selected_clas):
        """
        检查sg_otu_detail表里的一条记录，当他符合venn_table的第三列的时候，返回这条记录的id
        """
        LEVEL = {
            1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
            6: "f__", 7: "g__", 8: "s__", 9: "otu"
        }
        level = self.level + 1
        classify = list()
        """
        for i in range(1, 10):
            if LEVEL[i] not in result:
                raise Exception("sg_otu_detail表中，记录{}的字段{}缺失".format(result["_id"], LEVEL[i]))
        """
        """
        for i in range(1, level):
            classify.append(result[LEVEL[i]])
        """
        #  因为现在阶段otu_detail表还存在一些问题，分类等级还不齐全，所以还需要补全
        last_classify = ""
        for i in range(1, 10):
            if LEVEL[i] in result:
                if re.search("uncultured$", result[LEVEL[i]]) or re.search("Incertae_Sedis$", result[LEVEL[i]]) or re.search("norank$", result[LEVEL[i]]):
                    result[LEVEL[i]] = result[LEVEL[i]] + "_" + result[LEVEL[i - 1]]
        for i in range(1, level):
            if LEVEL[i] not in result:
                if last_classify == "":
                    last_classify = result[LEVEL[i - 1]]
                my_str = LEVEL[i] + "Unclasified_" + last_classify
            else:
                if not result[LEVEL[i]]:
                    if LEVEL[i] == "otu":
                        my_str = "otu__" + "Unclasified_" + last_classify
                    else:
                        my_str = LEVEL[i] + "Unclasified_" + last_classify
                else:
                    my_str = result[LEVEL[i]]
            classify.append(my_str)

        my_classify = "; ".join(classify)
        if my_classify in selected_clas:
            return result['_id']
        else:
            return None

    def add_sg_otu(self, otu_id):
        if not isinstance(otu_id, ObjectId):
            otu_id = ObjectId(otu_id)
        insert_data = {
            "project_sn": self.project_sn,
            "task_id": self.task_id,
            "from_id": otu_id,
            "name": "venn_otu_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
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
