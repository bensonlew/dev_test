# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import datetime
from biocluster.config import Config


class EstTTest(Base):
    def __init__(self, bind_object):
        super(EstTTest, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def get_another_name(self, name, group_list):
        another = ''
        for n in group_list:
            if n == name:
                pass
            else:
                another = n
        return another

    @report_check
    def add_est_t_test_detail(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            group_list = re.findall(r'mean\((.*?)\)', l)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                length = len(line_data)
                i = 1
                for name in group_list:
                    data = [("alpha_ttest_id", table_id), ("index_type", line_data[0]), ("qvalue", line_data[length-1]), ("pvalue", line_data[length-2])]
                    data.append(("category_name", name))
                    data.append(("compare_name", self.get_another_name(name, group_list)))
                    data.append(("mean", str('%0.5g' % float(line_data[i]))))
                    data.append(("sd", str('%0.5g' % float(line_data[i+1]))))
                    i += 2
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_alpha_ttest_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)

    @report_check
    def add_est_t_test_collection(self, params, group_id, from_est_id=0, name=None, group_name=None):
        if isinstance(from_est_id, StringTypes):
            from_est_id = ObjectId(from_est_id)
        else:
            raise Exception("est_id必须为ObjectId对象或其对应的字符串!")
        if group_id == "all":
            group_id = group_id
        else:
            group_id = ObjectId(group_id)
        collection = self.db["sg_alpha_diversity"]
        result = collection.find_one({"_id": from_est_id})
        project_sn = result['project_sn']
        task_id = result['task_id']
        otu_id = result['otu_id']
        level_id = result['level_id']
        desc = ""
        insert_data = {
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": otu_id,
                "alpha_diversity_id": from_est_id,
                "name": self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else "多样性指数T检验结果表",
                "level_id": int(level_id),
                "group_id": group_id,
                "compare_column": group_name,
                "status": "end",
                "desc": desc,
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_ttest"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
