# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import re
from biocluster.api.database.base import Base, report_check
import os
from biocluster.config import Config
from bson import regex
from pymongo import MongoClient
from bson import ObjectId

class PtCustomer(Base):
    '''
    将生成的tab文件导入mongo之ref的数据库中
    '''
    def __init__(self, bind_object):
        super(PtCustomer, self).__init__(bind_object)
        self.mongo_client = Config().mongo_client
        self.database = self.mongo_client['paternity_test']

        self.mongo_client_ref = Config().biodb_mongo_client
        self.database_ref = self.mongo_client_ref['paternity_test']


    # @report_check
    def add_pt_customer(self, main_id=None, customer_file=None):
        if customer_file == "None":
            self.bind_object.logger.info("缺少家系表")
        if main_id == "None":
            self.bind_object.logger.info("缺少主表id")
        with open(customer_file, 'r') as f:
            # num = 0
            for line in f:
                # num += 1
                # if num == 1: #csv格式的文件没有表头 所以不需要这一句
                # 	continue
                print line
                #line = line.decode("gb2312")
                line = line.decode("GB18030")
                line = line.strip()
                line = line.split(',')
                if line[0] == "序号":  # 去表头
                    continue
                if line[1] == "":
                    break
                if line[4] == "" or line[7] == "":
                    continue

                if len(line) >= 22 and line[21] != "":
                    family_name = "WQ" + line[8].split("-")[1] +"-"+ line[8].split("-")[-1] + "-" + line[5].split("-")[-1] + "-" + line[21].split("-")[-1]
                else:
                    family_name = "WQ" + line[8].split("-")[1] +"-"+ line[8].split("-")[-1] + "-" + line[5].split("-")[-1] + "-S"
                print len(line)
                collection = self.database["sg_pt_customer"]
                result = collection.find_one({"name": family_name})
                if result:
                    continue
                insert_data = {
                    "pt_datasplit_id": ObjectId(main_id),
                    "pt_serial_number": line[1],
                    "ask_person": line[2],
                    "mother_name": line[3],
                    "mother_type": line[4],
                    "mom_id": line[5],
                    "father_name": line[6],
                    "father_type": line[7],
                    "dad_id": line[8],
                    "ask_time": line[9],
                    "accept_time": line[10],
                    "result_time": line[11],
                    "name": family_name
                }
                try:
                    collection = self.database['sg_pt_customer']
                    collection.insert_one(insert_data)
                except Exception as e:
                    self.bind_object.logger.error('导入家系表表出错：{}'.format(e))
                else:
                    self.bind_object.logger.info("导入家系表成功")
        try:
            main_collection = self.database["sg_pt_datasplit"]
            self.bind_object.logger.info("开始刷新主表写状态")
            main_collection.update({"_id": ObjectId(main_id)},
                                    {"$set": {
                                        "desc": "pt_datasplit done, start pt_batch"}})
        except Exception as e:
            self.bind_object.logger.error("更新sg_pt_datasplit主表出错:{}".format(e))
        else:
            self.bind_object.logger.info("更新sg_pt_datasplit表格成功")

    def add_data_dir(self, dir_name, wq_dir, ws_dir, undetermined_dir):
        insert_data = {
            "data_name": dir_name,
            "wq_dir": wq_dir,
            "ws_dir": ws_dir,
            "undetermined_dir": undetermined_dir
        }
        try:
            collection = self.database["sg_med_data_dir"]
            if collection.find_one({"data_name": dir_name}):
                collection.update_one({"data_name": dir_name}, {'$set':
                                                            {"wq_dir": wq_dir,
                                                            "ws_dir": ws_dir,
                                                            "undetermined_dir": undetermined_dir}})
            else:
                collection.insert_one(insert_data)
        except Exception as e:
            self.bind_object.logger.info('导入拆分结果路径出错：{}'.format(e))
        else:
            self.bind_object.logger.info('导入拆分结果路径成功')

    def get_wq_dir(self, file_name):
        main_collection = self.database["sg_med_data_dir"]
        result = main_collection.find_one({"data_name": file_name})
        dir_list = []
        if result:
            dir_list.append(result["wq_dir"])
            dir_list.append(result["ws_dir"])
            dir_list.append(result["undetermined_dir"])
            return dir_list
        else:
            return dir_list

    def add_sample_type(self, file):
        insert =[]
        with open(file,'r') as f:
            for line in f:
                line = line.strip()
                line = line.split('\t')
                if re.match('WQ[0-9]*-.*',line[3]):
                    insert_data ={
                        "type": line[2],
                        "sample_id":line[3]
                    }
                    collection = self.database_ref['sg_pt_ref_main']
                    if collection.find_one({"sample_id": line[3]}):
                        pass
                    else:
                        insert.append(insert_data)
            if insert:
                try:
                    collection = self.database_ref['sg_pt_ref_main']
                    collection.insert_many(insert)
                except Exception as e:
                    self.bind_object.logger.error('导入ref类型出错：{}'.format(e))
                else:
                    self.bind_object.logger.info("导入ref类型成功")
            else:
                self.bind_object.logger.info("没有插入样本信息")




