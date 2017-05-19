# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# last_modify:20170519
from biocluster.api.database.base import Base, report_check
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
from biocluster.config import Config


class NiptAnalysis(Base):
    def __init__(self, bind_object):
        super(NiptAnalysis, self).__init__(bind_object)
        self.mongo_client = Config().mongo_client
        self.database = self.mongo_client['tsanger_nipt']  # 结果库
        self.mongo_client_ref = Config().biodb_mongo_client
        self.database_ref = self.mongo_client_ref['sanger_nipt_ref']  # 参考库

    # @report_check
    def add_zz_result(self, file_path, table_id=None, major=False):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        with open(file_path, "rb") as r:
            data1 = r.readlines()[1:]
            for line1 in data1:
                temp1 = line1.rstrip().split("\t")
                data = [("nipt_task_id", table_id), ("sample_id", str(temp1[0])), ("zz", eval(temp1[1]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database["sg_nipt_zz_result"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    # @report_check
    def add_z_result(self, file_path, table_id=None, major=False):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            data1 = r.readlines()[1:]
            for line in data1:
                temp = line.rstrip().split("\t")
                data = [("nipt_task_id", table_id), ("sample_id", str(temp[0])), ("chr", int(temp[1])),
                        ("cn", eval(temp[2])), ("bin", int(temp[3])), ("n", int(temp[4])), ("sd", eval(temp[5])),
                        ("mean", eval(temp[6])), ("z", eval(temp[7]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database["sg_nipt_z_result"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list


    def add_bed_file(self, file_path):
        """
        用于添加nipt分析shell部分生成的bed文件到mongo表中
        """
        data_list = []
        with open(file_path, 'rb') as r:
            data1 = r.readlines()
            for line in data1:
                temp = line.rstrip().split("\t")
                data = [("chr", int(temp[0])),
                        ("start", int(temp[1])), ("end", int(temp[2])), ("gc", eval(temp[3])), ("map", eval(temp[4])),
                        ("pn", eval(temp[5])), ("reads", int(temp[6])), ("sample_id", str(temp[7]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database_ref["sg_nipt_bed"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息到sg_nipt_bed成功!" % file_path)
        return True
