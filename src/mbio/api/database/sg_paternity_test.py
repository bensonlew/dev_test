# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import datetime
import random

from biocluster.api.database.base import Base, report_check
import re
from biocluster.config import Config
from pymongo import MongoClient
import gridfs
from mainapp.libs.param_pack import param_pack

class SgPaternityTest(Base):
	'''
	将前端需要调用的结果文件导入mongo数据库，之结果保存的tsanger collection
	'''
	def __init__(self, bind_object):
		super(SgPaternityTest, self).__init__(bind_object)
		# self._db_name = Config().MONGODB
		self.mongo_client = MongoClient(Config().MONGO_URI)
		self.database = self.mongo_client['tsanger_paternity_test']

	@report_check
	def add_sg_task(self,dad,mom,preg,ref_fasta,targets_bedfile,ref_point,fastq_path):
		# family_no = re.search("WQ([1-9].*)-F", dad)
		# "name": family_no.group(1)
		task_id = self.bind_object.id
		insert_data = {
			"project_sn": self.bind_object.sheet.project_sn,
			"task_id": task_id,
			"member_id": self.bind_object.sheet.member_id,
			"dad_id": dad,
			"mom_id": mom,
			"preg_id": preg,
			"created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
			"ref_fasta": ref_fasta,
			"targets_bedfile": targets_bedfile,
			"ref_point": ref_point,
			"fastq_path":fastq_path,

		}
		try:
			collection = self.database['sg_pt_task']
			collection.insert_one(insert_data)
		except Exception as e:
			self.bind_object.logger.error('导入task表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入task表成功")
		return task_id

	# "status": "end",
	@report_check
	def add_pt_family(self, task_id, err_min, dedup):
		params = dict()
		params['err_min'] = err_min
		params['dedup'] = dedup
		name = 'err-' + str(err_min) + '_dedup-'+ str(dedup)
		insert_data = {
			"task_id": task_id,
			"name": name,
			"created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
		}

		collection = self.database['sg_pt_family']
		new_params = param_pack(params)
		insert_data["params"] = new_params
		# collection.insert_data["params"] = params
		try:
			flow_id = collection.insert_one(insert_data).inserted_id
			# collection.insert_one(insert_data)
		except Exception as e:
			self.bind_object.logger.error('导入任务主表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入任务主表成功")
		return flow_id

	@report_check
	def add_sg_pt_family_detail(self,file_path,flow_id):
		self.bind_object.logger.info("开始导入调试表")
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "chrom":
					continue
				insert_data = {
					"task_id": self.bind_object.id,
					"family_id": flow_id,
					"chrom": line[0],
					"pos":line[1],
					"dad_id": line[2],
					"dad_ref": line[3],
					"dad_alt": line[4],
					"dad_dp": line[5],
					"dad_ref_dp": line[6],
					"dad_alt_dp": line[7],
					"dad_rf": line[8],
					"dad_geno": line[9],
					"dad_geno_bases": line[10],
					"preg_id": line[11],
					"preg_ref": line[12],
					"preg_alt": line[13],
					"preg_dp": line[14],
					"preg_ref_dp": line[15],
					"preg_alt_dp": line[16],
					"preg_rf": line[17],
					"preg_geno": line[18],
					"preg_geno_bases": line[19],
					"mom_id": line[20],
					"mom_ref": line[21],
					"mom_alt": line[22],
					"mom_dp": line[23],
					"mom_ref_dp": line[24],
					"mom_alt_dp": line[25],
					"mom_rf": line[26],
					"mom_geno": line[27],
					"mom_geno_bases": line[28],
					"reg": line[29],
					"from": line[30],
					"to": line[31],
					"rs": line[32],
					"hapmap_rf": line[33],
					"hapmap_geno": line[34],
					"n": line[35],
					"mj_ref": line[36],
					"pA": line[37],
					"pG": line[38],
					"pC": line[39],
					"pT": line[40],
					"mj_dp": line[41],
					"mj_gene": line[42],
					"is_test": line[43],
					"is_mis": line[44],
					"mustbe": line[45],
					"mustnotbe": line[46],
					"good": line[47],
					"pi": line[48]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_family_detail']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入调试表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入调试表格成功")

	@report_check
	def add_pt_figure(self, output_dir,flow_id):
		self.bind_object.logger.info("图片开始导入数据库")
		fs = gridfs.GridFS(self.database)
		family_fig = fs.put(open(output_dir + '/family.png', 'r'))
		figure1 = fs.put(open(output_dir + '/fig1.png', 'r'))
		figure2 = fs.put(open(output_dir + '/fig2.png', 'r'))
		preg_percent = fs.put(open(output_dir + '/preg_percent.png', 'r'))
		update_data = {
			"task_id": self.bind_object.id,
			"family_id": flow_id,
			'family_fig': family_fig,
			'figure1': figure1,
			'figure2': figure2,
			'preg_percent': preg_percent
		}
		collection = self.database["sg_pt_family_figure"]
		collection.insert_one(update_data)

	@report_check
	def add_analysis_tab(self, file_path,flow_id):
		self.bind_object.logger.info("开始导入分析结果表")
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "dad.id":
					continue
				insert_data = {
					"task_id": self.bind_object.id,
					"family_id": flow_id,
					"dad_id": line[0],
					"test_pos_n": line[1],
					"err_pos_n": line[2],
					"err_rate": line[3],
					"fq": line[4],
					"dp": line[5],
					"eff_rate": line[6],
					"ineff_rate": line[7],
					"result": line[8]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_family_analysis']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入分析结果表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入分析结果表格成功")

	@report_check
	def add_info_detail(self, file_path,flow_id):
		self.bind_object.logger.info("开始导入信息分析表")
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "bed.preg.id":
					continue
				insert_data = {
					"task_id": self.bind_object.id,
					"family_id": flow_id,
					"preg_id": line[0],
					"dp_preg": line[1],
					"percent": line[2],
					"error": line[3],
					"s_signal": line[4],
					"mom_id": line[5],
					"dp_mom": line[6],
					"mom_preg": line[7]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_family_result_info']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入信息分析表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入信息分析表格成功")

	@report_check
	def add_test_pos(self, file_path, flow_id):
		self.bind_object.logger.info("开始导入位点信息表")
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "检测位点编号":
					continue
				insert_data = {
					"task_id": self.bind_object.id,
					"family_id": flow_id,
					"test_no": line[0],
					"chrom": line[1],
					"dad_geno": line[2],
					"mom_geno": line[3],
					"preg_geno": line[4],
					"is_mis": line[5]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_family_test_pos']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入位点信息表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入位点信息表格成功")
