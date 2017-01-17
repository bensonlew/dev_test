# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import re
from biocluster.api.database.base import Base, report_check
import os
from biocluster.config import Config
from bson import regex
from pymongo import MongoClient

class TabFile(Base):
	'''
	将生成的tab文件导入mongo之ref的数据库中
	'''
	def __init__(self, bind_object):
		super(TabFile, self).__init__(bind_object)
		# self._db_name = Config().MONGODB
		self.mongo_client = MongoClient(Config().MONGO_BIO_URI)
		self.database = self.mongo_client['sanger_paternity_test']


	@report_check
	def add_sg_pt_tab_detail(self,file_path):
		self.bind_object.logger.info("开始导入tab表")
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
				collection = self.database['sg_pt_ref']
				collection.insert_many(sg_pt_tab_detail)
			except Exception as e:
				self.bind_object.logger.error('导入tab表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入tab表格成功")

	@report_check
	def tab_exist(self, sample):
		self.bind_object.logger.info('开始检测tab表格')
		collection = self.database['sg_pt_ref']
		result = collection.find_one({'sample_id': sample})
		if not result:
			self.bind_object.logger.info("样本{}不在数据库中，开始进行转tab文件并入库流程".format(sample))
		else:
			self.bind_object.logger.info("样本{}已存在数据库中，不进行fastq转tab流程".format(sample))
		return result


	def export_tab_file(self, sample, dir):
		collection = self.database['sg_pt_ref']
		search_result = collection.find({"sample_id": sample})  # 读出来是个地址
		temp = collection.find_one({"sample_id":sample})
		if temp:
			final_result = search_result
			file = os.path.join(dir, sample + '.tab')
		else:
			sample_other_name = sample + '1'
			if collection.find_one({"sample_id": sample_other_name}):
				final_result = collection.find({"sample_id": sample_other_name})
				file = os.path.join(dir, sample_other_name + '.tab')
			else:
				raise Exception('意外报错：没有在数据库中搜到相应sample')
		with open(file, 'w+') as f:
			for i in final_result:
					f.write(i['sample_id'] + '\t' + i['chrom'] + '\t' + i['pos'] + '\t'
				            + i['ref'] + '\t' + i['alt'] + '\t' + i['dp'] + '\t'
				            + i['ref_dp'] + '\t' + i['alt_dp'] + '\n')
		if os.path.getsize(file):
			return file
		else:
			raise Exception('报错：样本数据{}的tab文件为空，可能还未下机'.format(sample))


	def dedup_sample(self, num):
		collection = self.database['sg_pt_ref']
		param = "WQ{}-F".format(num) + '.*'
		sample = []

		for u in collection.find({"sample_id": {"$regex": param}}):
			sample.append(u['sample_id'])
		sample_new = list(set(sample))
		return sample_new

	def dedup_fuzzy_sample(self, num, dad_id):
		collection = self.database['sg_pt_ref']
		param = "WQ[1-9]*{}[1-9]*-F".format(num)
		sample = []

		for u in collection.find({"sample_id": {"$regex": param}}):
			if u['sample_id'] != dad_id:
				sample.append(u['sample_id'])
		sample_new = list(set(sample))
		if sample_new:
			return sample_new
