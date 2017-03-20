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
		# self._db_name = Config().MONGODB
		# self.mongo_client = MongoClient(Config().MONGO_BIO_URI)
		# self.database = self.mongo_client['sanger_paternity_test_v2']
		self.mongo_client = MongoClient(Config().MONGO_URI)
		self.database = self.mongo_client['tsanger_paternity_test_v2']


	# @report_check
	def add_pt_customer(self, main_id=None, customer_file=None):
		if customer_file == "None":
			self.bind_object.logger.info("缺少家系表")
		if main_id == "None":
			self.bind_object.logger.info("缺少主表id")
		with open(customer_file, 'r') as f:
			for line in f:
				line = line.decode("gb2312")
				line = line.strip()
				line = line.split('\t')
				family_name = line[8] + "-" + line[5].split("-")[-1] + line[20].split("-")[-1]
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
					"mother_id": line[5],
					"father_name": line[6],
					"father_type": line[7],
					"father_id": line[8],
					"ask_time": line[9],
					"accept_time": line[10],
					"result_time": line[11],
					"son_id": line[20],
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
										"status": "pt_datasplit done, start pt_batch"}})
		except Exception as e:
			self.bind_object.logger.error("更新sg_pt_datasplit主表出错:{}".format(e))
		else:
			self.bind_object.logger.info("更新sg_pt_datasplit表格成功")

	def update_flow_status(self,batch_id):
		try:
			main_collection = self.database["sg_pt_datasplit"]
			main_collection.update({"_id": ObjectId(batch_id)},{"$set": {"status": "end"}})
		except Exception as e:
			self.bind_object.logger.error("更新大流程主表状态出错:{}".format(e))
		else:
			self.bind_object.logger.info("更新大流程主表状态成功")



