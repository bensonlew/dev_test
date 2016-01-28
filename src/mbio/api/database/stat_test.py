# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
import datetime
from bson.son import SON


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
            if not re.match(r'\t',l):
                raise Exception("文件%s格式不正确，请选择正确的差异性比较统计结果表格文件" % file_path)
            group = re.findall(r'mean\W', l)
            group_list = []
            for n in group:
                group_list.append(n.split("mean(")[-1])
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                length = len(line_data)
                i = 1
                for name in group_list:
                    data = [("species_check_id", table_id),("species_name", line_data[0]),("qvalue", line_data[length-1]),("pvalue", line_data[length-2])]                    
                    data.append("category_name", name)
                    data.append("mean", line_data[i])
                    data.append("sd", line_data[i+1])
                    i += 1
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
            if not re.match(r'\t',l):
                raise Exception("文件%s格式不正确，请选择正确的差异性比较统计结果表格文件" % file_path)
            sample = re.findall(r'propotion\W', l)
            sample_list = []
            for n in sample:
                sample_list.append(n.split("propotion(")[-1])
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = [("species_check_id", table_id),("species_name", line_data[0]),("qvalue", line_data[5]),("pvalue", line_data[4]),("specimen_name", sample_list[0]),("propotion", line_data[2])]
                data_son = SON(data)
                data_list.append(data_son)
                data1 = [("species_check_id", table_id),("species_name", line_data[0]),("qvalue", line_data[5]),("pvalue", line_data[4]),("specimen_name", sample_list[1]),("propotion", line_data[3])]
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
            if not re.match(r'\t',l):
                raise Exception("文件%s格式不正确，请选择正确的差异性比较统计结果表格文件" % file_path)
            group = r.findall(r'min\W', l)
            group_list = []
            for n in group:
                group_list.append(n.split("min(")[-1])
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                i = 1
                for name in group_list:
                    data = [("species_check_id", table_id),("species_name", line_data[0])]
                    data.append("category_name", name)
                    data.append("min", line_data[i])
                    data.append("q1", line_data[i+1])
                    data.append("median", line_data[i+1])
                    data.append("q3", line_data[i+1])
                    data.append("max", line_data[i+1])
                    i += 1
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_boxfile"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
















