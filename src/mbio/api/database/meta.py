# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import json
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
import datetime
from bson.son import SON
from types import StringTypes
# from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort, param_pack


class Meta(Base):

    def __init__(self, bind_object):
        super(Meta, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_otu_table(self, file_path, major=False, otu_id=None, from_out_table=0, rep_path=None, task_id=None,
                      name=None, params=None, spname_spid=None):
        if task_id is None:
            task_id = self.bind_object.sheet.id
        # if level not in range(1, 10):
        #     raise Exception("level参数%s为不在允许范围内!" % level)
        if from_out_table != 0 and not isinstance(from_out_table, ObjectId):
            if isinstance(from_out_table, StringTypes):
                from_out_table = ObjectId(from_out_table)
            else:
                raise Exception("from_out_table必须为ObjectId对象或其对应的字符串!")
        if major:
            if spname_spid and params:
                group_detail = {'All': [str(i) for i in spname_spid.values()]}
                params['group_detail'] = group_detail_sort(group_detail)
                #params['level_id'] = 9  # modified by hongdongxuan 20170303
            if task_id is None:
                task_id = self.bind_object.sheet.id
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "name": self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else "OTU_Taxon_Origin",
                "from_id": from_out_table,
                "status": "end",
                "level_id": json.dumps([9]),
                "type": "otu_statistic,otu_venn,otu_group_analysis",
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            }
            collection = self.db["sg_otu"]
            otu_id = collection.insert_one(insert_data).inserted_id
            params['otu_id'] = str(otu_id)
            insert_data["from_id"] = str(otu_id)
            new_params = param_pack(params)
            insert_data["params"] = new_params
            try:
                collection.find_one_and_update({"_id": otu_id}, {'$set': insert_data})
            except Exception as e:
                raise Exception('更新OTU表params出错:{}'.format(e))
            insert_data["from_id"] = str(otu_id)
            collection.find_one_and_update({"_id": otu_id}, {'$set': insert_data})
        else:
            if otu_id is None:
                raise Exception("major为False时需提供otu_id!")
        data_list = []
        # 读代表序列
        otu_reps = {}
        if rep_path:
            with open(rep_path, 'r') as f:
                seq_id = ""
                seq = ''
                while True:
                    line = f.readline().strip('\n')
                    if not line:
                        otu_reps[seq_id] = seq
                        break
                    m = re.match(r">(\S+)\s", line)
                    if m:
                        otu_reps[seq_id] = seq
                        seq_id = m.group(1)
                        seq = ''
                    else:
                        seq = seq + line
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"^OTU ID", l):
                raise Exception("文件%s格式不正确，请选择正确的OTU表格文件" % file_path)
            sample_list = l.split("\t")
            sample_list.pop()
            sample_list.pop(0)
            sample_data = []
            for sample in sample_list:
                sample_data.append({"otu_id": otu_id, "specimen_id": spname_spid[sample]})
            collection = self.db["sg_otu_specimen"]
            collection.insert_many(sample_data)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                classify = line_data.pop()
                classify_list = re.split(r"\s*;\s*", classify)
                otu_list = [("task_id", task_id), ("otu_id", otu_id)]
                if rep_path:
                    otu_list.append(("otu_rep", otu_reps[line_data[0]]))
                for cf in classify_list:
                    if cf != "":
                        otu_list.append((cf[0:3], cf))
                i = 0
                otu_list.append(("otu", line_data[0]))
                for sample in sample_list:
                    i += 1
                    otu_list.append((sample, int(line_data[i])))
                data = SON(otu_list)
                data_list.append(data)
        try:
            collection = self.db["sg_otu_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入OTU表格%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入OTU表格%s信息成功!" % file_path)
        return otu_id
