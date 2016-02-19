# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import gridfs
import pymongo


class StatTest(Base):
    def __init__(self, bind_object):
        super(StatTest, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_species_difference_check_detail(self, file_path, table_id):
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
                    data = [("species_check_id", table_id),("species_name", line_data[0]),("corrected_pvalue", line_data[length-1]),("pvalue", line_data[length-2])]
                    data.append(("category_name", name))
                    data.append(("mean", line_data[i]))
                    data.append(("sd", line_data[i+1]))
                    i += 2
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
    
    @report_check
    def add_twosample_species_difference_check(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            s1 = re.search(r'propotion\((.*)\)\W+propotion\((.*)\)', l)
            sample = s1.groups(0)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = [("species_check_id", table_id), ("species_name", line_data[0]), ("corrected_pvalue", line_data[4]),
                        ("pvalue", line_data[3]), ("specimen_name", sample[0]), ("propotion", line_data[1])]
                data_son = SON(data)
                data_list.append(data_son)
                data1 = [("species_check_id", table_id), ("species_name", line_data[0]), ("corrected_pvalue", line_data[4]),
                         ("pvalue", line_data[3]), ("specimen_name", sample[1]), ("propotion", line_data[2])]
                data_son1 = SON(data1)
                data_list.append(data_son1)
        try:
            collection = self.db["sg_species_difference_check_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)

    @report_check
    def add_species_difference_check_boxplot(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            group_list = re.findall(r'min\((.*?)\)', l)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                i = 1
                for name in group_list:
                    data = [("species_check_id", table_id), ("species_name", line_data[0])]
                    data.append(("category_name", name))
                    data.append(("min", line_data[i]))
                    data.append(("q1", line_data[i+1]))
                    data.append(("median", line_data[i+2]))
                    data.append(("q3", line_data[i+3]))
                    data.append(("max", line_data[i+4]))
                    i += 5
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_boxplot"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def add_species_difference_lefse_detail(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    data = [("species_lefse_id", table_id), ("species_name", line_data[0]),
                            ("category_name", line_data[2]), ("median", line_data[1]), ("lda", line_data[3]),
                            ("pvalue", line_data[4])]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_lefse_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def update_species_difference_lefse(self, lda_png_path, lda_cladogram_path, table_id):
        collection = self.db["sg_species_difference_lefse"]
        fs = gridfs.GridFS(self.db)
        ldaid = fs.put(open(lda_png_path, 'r'))
        cladogramid = fs.put(open(lda_cladogram_path, 'r'))
        try:
            collection.update({"_id": ObjectId(table_id)}, {"$set": {"lda_png_id": ldaid, "lda_cladogram_id": cladogramid}})
        except Exception, e:
            self.bind_object.logger.error("导入%s和%s信息出错:%s" % (lda_png_path, lda_cladogram_path, e))
        else:
            self.bind_object.logger.info("导入%s和%s信息成功!" % (lda_png_path, lda_cladogram_path))






