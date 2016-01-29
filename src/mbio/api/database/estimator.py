# -*- coding: utf-8 -*-
# __author__ = 'yuguo'
from biocluster.api.database.base import Base, report_check
import re
# from bson.objectid import ObjectId
import datetime
from bson.son import SON
# from types import StringTypes


class Estimator(Base):
    def __init__(self, bind_object):
        super(Estimator, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_est_table(self, file_path, level, major=False, otu_id=None, est_id=None, task_id=None, name=None, params=None):
        if level not in range(1, 10):
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
                "name": name if name else "estimators_origin",
                "level_id": level,
                "status": "end",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            if params is not None:
                insert_data['params'] = params
            collection = self.db["sg_alpha_diversity"]
            est_id = collection.insert_one(insert_data).inserted_id
        else:
            if est_id is None:
                raise Exception("major为False时需提供est_id!")
        # insert detail
        with open(file_path, 'r') as f:
            l = f.readline().strip('\n')
            if not re.match(r"^sample", l):
                raise Exception("文件%s格式不正确，请选择正确的estimator表格文件" % file_path)
            est_list = l.split("\t")
            # est_list.pop()
            est_list.pop(0)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                sample_name = line_data.pop(0)
                # line_data.pop()
                data = [("alpha_diversity_id", est_id), ("specimen_name", sample_name)]
                i = 0
                for est in est_list:
                    data.append((est, float(line_data[i])))
                    i += 1
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_alpha_diversity_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入estimator%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入estimator%s信息成功!" % file_path)
