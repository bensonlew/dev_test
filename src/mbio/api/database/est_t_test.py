# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON


class EstTTest(Base):
    def __init__(self, bind_object):
        super(EstTTest, self).__init__(bind_object)
        self._db_name = "sanger"

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
                    data = [("alpha_est_t_test_id", table_id), ("index_type", line_data[0]), ("qvalue", line_data[length-1]), ("pvalue", line_data[length-2])]
                    data.append(("category_name", name))
                    data.append(("compare_name", self.get_another_name(name, group_list)))
                    data.append(("mean", str('%0.5g' % float(line_data[i]))))
                    data.append(("sd", str('%0.5g' % float(line_data[i+1]))))
                    i += 2
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_alpha_est_t_test_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
