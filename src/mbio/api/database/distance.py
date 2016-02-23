# -*- coding: utf-8 -*-
# __author__ = 'yuguo'
from biocluster.api.database.base import Base, report_check
import re
# from bson.objectid import ObjectId
import datetime
from bson.son import SON
# from types import StringTypes


class Distance(Base):
    def __init__(self, bind_object):
        super(Distance, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_dist_table(self, file_path, major=False, level=None, dist_id=None, otu_id=None, task_id=None, name=None, params=None):
        if level and level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        data_list = []
        # insert major
        if major:
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "otu_id": otu_id,
                "level_id": level,
                "name": name if name else "distance_origin",
                "status": "end",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            collection = self.db["sg_beta_specimen_distance"]
            dist_id = collection.insert_one(insert_data).inserted_id
        else:
            if dist_id is None:
                raise Exception("major为False时需提供dist_id!")
        # insert detail
        with open(file_path, 'r') as f:
            l = f.readline().strip('\n')
            if not re.match(r"^\t", l):
                raise Exception("文件%s格式不正确，请选择正确的distance表格文件" % file_path)
            sample_list = l.split("\t")
            sample_list.pop(0)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                sample_name = line_data.pop(0)
                data = [("specimen_distance_id", dist_id), ("specimen_name", sample_name)]
                i = 0
                for smp in sample_list:
                    data.append((smp, float(line_data[i])))
                    i += 1
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_beta_specimen_distance_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入distance%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入distance%s信息成功!" % file_path)
        return dist_id