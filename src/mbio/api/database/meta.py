# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
import datetime
from bson.son import SON
from types import StringTypes


class Meta(Base):
    def __init__(self, bind_object):
        super(Meta, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_otu_table(self, file_path, level, from_out_table=0, task_id=None, name=None, params=None):
        if level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if from_out_table != 0 and not isinstance(from_out_table, ObjectId):
            if isinstance(from_out_table, StringTypes):
                from_out_table = ObjectId(from_out_table)
            else:
                raise Exception("from_out_table必须为ObjectId对象或其对应的字符串!")
        if task_id is None:
            task_id = self.bind_object.sheet.id
        data_list = []
        otu_id = ""
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"^OTU ID", l):
                raise Exception("文件%s格式不正确，请选择正确的OTU表格文件" % file_path)
            sample_list = l.split("\t")
            sample_list.pop()
            sample_list.pop(0)
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "name": name if name else "otu_taxon_origin",
                "from_id": from_out_table,
                "level": level,
                "status": "end",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            }
            collection = self.db["sg_otu"]
            otu_id = collection.insert_one(insert_data).inserted_id

            sample_data = []
            for sample in sample_list:
                sample_data.append({"otu_id": otu_id, "specimen_name": sample})
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
