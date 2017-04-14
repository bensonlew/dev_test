# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import re
from biocluster.api.database.base import Base, report_check
import os
from biocluster.config import Config
from bson import regex
from bson import ObjectId
from pymongo import MongoClient

class TabFile(Base):
	'''
	将生成的tab文件导入mongo之ref的数据库中
	'''
	def __init__(self, bind_object):
		super(TabFile, self).__init__(bind_object)
		# self._db_name = Config().MONGODB
		self.mongo_client = MongoClient(Config().MONGO_BIO_URI)
		self.database = self.mongo_client['sanger_paternity_test_v2']


	# @report_check
	def add_pt_tab(self,sample,batch_id):
		if "-F" in sample:
			analysised = "no"
		else:
			analysised = "None"
		with open(sample, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] != '':
					sample_name = line[0]
					insert_data = {
						"analysised": analysised,
						"batch_id": ObjectId(batch_id)
					}
				break
			try:
				collection = self.database['sg_pt_ref_main']
				collection.find_one_and_update({"sample_id": sample_name},{'$set':insert_data})
			except Exception as e:
				self.bind_object.logger.error('导入tab主表出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入tab主表成功")

	def update_pt_tab(self,sample):
		try:
			collection = self.database['sg_pt_ref_main']
			collection.update({"sample_id": sample}, {'$set':{"analysised": "yes"}})
		except Exception as e:
			self.bind_object.logger.error('更新tab主表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("更新tab主表成功")


	# @report_check
	def add_sg_pt_tab_detail(self,file_path):
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
					"alt_dp": line[7],
				}
				sg_pt_tab_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_ref']
				collection.insert_many(sg_pt_tab_detail)
			except Exception as e:
				self.bind_object.logger.error('导入tab表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入tab表格成功")

	# @report_check
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
		param = "WQ\d{1,}%d\d{1,}-F" % (num)
		sample = []

		for u in collection.find({"sample_id": {"$regex": param}}):
			if u['sample_id'] != dad_id or u['sample_id'] != dad_id + '1':
				sample.append(u['sample_id'])
		sample_new = list(set(sample))
		if sample_new:
			return sample_new

	def sample_qc(self, file, sample_id):
		qc_detail = list()
		with open(file,'r') as f:
			for line in f:
				line = line.strip()
				line = line.split(":")

				if line[0] == 'dp':
					if float(line[1]) >=40:
						color = "green"
					elif float(line[1]) < 30:
						color = "red"
					else:
						color = "yellow"
				elif line[0] == "num__chrY":
					if "-F" in sample_id:
						if float(line[1]) < 10:
							color = 'red'
						else:
							color = 'green'
					elif "-M" in sample_id:
						if float(line[1]) >= 3:
							color = 'red'
						elif float(line[1]) == 0:
							color ='green'
						else:
							color = "yellow"
					elif "-S" in sample_id:
						line[1]='/'
						color = ''
				elif line[0] == 'num':
					num_M = int(line[1])/1000000
					if "-M" in sample_id or "-F" in sample_id:
						if num_M <3:
							color = "red"
						elif 3 <= num_M <=4:
							color = 'yellow'
						else:
							color = 'green'
					else:
						if num_M <=7.5:
							color = 'red'
						elif num_M > 8.5:
							color = 'green'
						else:
							color = 'yellow'
				else:
					color = ''

				insert_data = {
					"qc": line[0],
					"value": line[1],
					"sample_id":sample_id,
					"color":color
				}
				qc_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_qc']
				collection.insert_many(qc_detail)
			except Exception as e:
				self.bind_object.logger.error('导入qc表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入qc表格成功")

	def sample_qc_addition(self,sample_id):
		collection = self.database['sg_pt_qc']
		insert = []

		find_n_hit = collection.find_one({"sample_id":sample_id, 'qc':'n_hit'})
		n_hit = float(find_n_hit['value'])
		find_num = collection.find_one({"sample_id":sample_id, "qc":'num'})
		num = float(find_num['value'])
		ot = n_hit/num
		if ot <= 0.015:
			color_ot = 'red'
		elif 0.015 < ot <= 0.025:
			color_ot = 'yellow'
		elif ot > 0.025:
			color_ot = 'green'

		find_n_hit_dedup = collection.find_one({"sample_id": sample_id, 'qc': 'n_hit_dedup'})
		n_hit_dedup = float(find_n_hit_dedup['value'])
		ot_dedup = n_hit_dedup/n_hit

		if ot_dedup<=0.2:
			color_ot_dedup = 'red'
		elif 0.2<ot_dedup<=0.3:
			color_ot_dedup = 'yellow'
		elif ot_dedup >0.3:
			color_ot_dedup = 'green'

		insert_data1 = {
			"qc":"ot",
			"value":ot,
			"sample_id":sample_id,
			"color": color_ot
		}
		insert.append(insert_data1)

		insert_data2 = {
			"qc": "ot_dedup",
			"value": ot_dedup,
			"sample_id": sample_id,
			"color": color_ot_dedup
		}
		insert.append(insert_data2)
		try:
			collection.insert_many(insert)
		except Exception as e:
			self.bind_object.logger.error('计算并导入ot出错：{}'.format(e))
		else:
			self.bind_object.logger.info("计算并导入ot成功")


	def family_unanalysised(self):
		family_id = []
		collection = self.database['sg_pt_ref_main']
		sample = collection.find({"analysised":'no',"type":{"$regex":"(p)?pt$"}})
		for i in sample:
			dad_id = []
			mom_id =[]
			preg_id =[]
			m = re.search(r'WQ([0-9]*)-F.*', i['sample_id'])
			family = m.group(1)
			dad_id.append(i['sample_id'])
			dad_id = list(set(dad_id))
			mom = "WQ" + family + "-M.*"
			sample_mom = collection.find({"sample_id": {"$regex": mom},"analysised":"None"})
			for s in sample_mom:
				mom_id.append(s['sample_id'])
				mom_id = list(set(mom_id))
			preg = "WQ" +family + "-S.*"
			sample_preg = collection.find({"sample_id": {"$regex": preg},"analysised":"None"})
			for n in sample_preg:
				preg_id.append(n['sample_id'])
				preg_id = list(set(preg_id))

			if sample_mom and sample_preg:
				for dad in dad_id:
					for mom in mom_id:
						for preg in preg_id:
							family_member = []
							family_member.append(dad)
							family_member.append(mom)
							family_member.append(preg)

							family_id.append(family_member)
				# if dad_id not in family_id and mom_id not in family_id and preg_id not in family_id:
				# 	family_id.append(dad_id)
				# 	family_id.append(mom_id)
				# 	family_id.append(preg_id)
				# 	final.append(family_id)
			else:
				self.bind_object.logger.info("家系数据还未全部下机")
				continue
		return family_id

	def type(self,sample_id):
		collection = self.database['sg_pt_ref_main']
		sample = collection.find_one({"sample_id": sample_id})
		print sample_id
		return sample['type']



