# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import datetime
import random

from mainapp.config.db import get_mongo_client
import os
from biocluster.config import Config
from pymongo import MongoClient

class PaternityTest(object):
	'''
	写入流程主表，从任务主表中提取父母本编号信息等
	'''
	def __init__(self):
		self.mongo_client = MongoClient(Config().MONGO_URI)
		self.database = self.mongo_client['tsanger_paternity_test']

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
		collection = self.database['sg_pt_family_main']
		task_info = collection.find_one({"task_id": task})
		#新建一个字典包含所需信息，然后返回这个字典
		info = dict(
			dad_id=task_info['dad_id'],
			mom_id=task_info['mom_id'],
			preg_id=task_info['preg_id'],
			ref_fasta=task_info['ref_fasta'],
			targets_bedfile=task_info['targets_bedfile'],
			ref_point=task_info['ref_point'],
			project_sn=task_info['project_sn']
		)
		return info