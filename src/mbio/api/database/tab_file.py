# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config
from pymongo import MongoClient

class TabFile(Base):
	'''
	将生成的tab文件导入mongo数据库
	'''
	def __init__(self, bind_object):
		super(TabFile, self).__init__(bind_object)
		# self._db_name = Config().MONGODB
		self.mongo_client = MongoClient(Config().MONGO_BIO_URI)
		self.database = self.mongo_client['sanger_paternity_test']


	@report_check
	def add_sg_pt_tab_detail(self,file_path):
		self.bind_object.logger.info("开始导入tab_detail表")
		sg_pt_tab_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				insert_data = {
					"sample_id": line[0],
					"chrom":line[1],
					"pos": line[2],
					"ref": line[3],
					"alt": line[4],
					"dp": line[5],
					"ref_dp": line[6],
					"alt_dp": line[7]
				}
				sg_pt_tab_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_tab_detail']
				collection.insert_many(sg_pt_tab_detail)
			except Exception as e:
				self.bind_object.logger.error('导入tab表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入tab表格成功")

	@report_check
	def tab_exist(self, sample):
		self.bind_object.logger.info('开始调用tab表格')
		collection = self.database['sg_pt_tab_detail']
		result = collection.find_one({'sample_id': sample})
		if not result:
			self.bind_object.logger.info("样本{}不在数据库中，开始进行转tab文件并入库流程".format(sample))
		else:
			self.bind_object.logger.info("样本{}已存在数据库中，不进行fastq转tab流程".format(sample))
		return result