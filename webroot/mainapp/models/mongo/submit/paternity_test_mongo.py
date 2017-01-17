# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import datetime
import random

from mainapp.config.db import get_mongo_client
import os
from bson import SON
from biocluster.config import Config
from pymongo import MongoClient

class PaternityTest(object):
	'''
	写入流程主表，从任务主表中提取父母本编号信息等
	'''
	def __init__(self):
		self.mongo_client = MongoClient(Config().MONGO_URI)
		self.database = self.mongo_client['tsanger_paternity_test']

		self.mongo_client_get_tab = MongoClient(Config().MONGO_BIO_URI)
		self.database_tab = self.mongo_client_get_tab['sanger_paternity_test']

	def add_pt_task_main(self, task, err_min):
		if task is None:
			raise Exception('未获取到任务id')
		flow_id = "%s_%s_%s" % (task, random.randint(1, 10000), random.randint(1, 10000))
		insert_data = {
			"task_id": task,
			"flow_id": flow_id,
			"err_min": err_min,
			"created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		}
		collection = self.database['sg_pt_task_main']
		collection.insert_one(insert_data)
		return flow_id

	def get_query_info(self,task):
		if task is None:
			raise Exception('未获取到任务id')
		collection = self.database['sg_pt_task']
		task_info = collection.find({"task_id": task})
		for info in task_info:
		# 	print i
		# #新建一个字典包含所需信息，然后返回这个字典
		# 	info = dict(
		# 		dad_id=i[u'dad_id'],
		# 		mom_id=i[u'mom_id'],
		# 		preg_id=i[u'preg_id'],
		# 		ref_fasta=i[u'ref_fasta'],
		# 		targets_bedfile=i[u'targets_bedfile'],
		# 		ref_point=i[u'ref_point'],
		# 		project_sn=i[u'project_sn'],
		# 		fastq_path=i[u'fastq_path']
		# 	)
			return info

	def insert_main_table(self, collection,data):
		return self.database[collection].insert_one(SON(data)).inserted_id
