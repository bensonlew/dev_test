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
		self.database = self.mongo_client['tsanger_paternity_test_v2']

	@report_check
	def add_sg_father(self,dad,mom,preg):
		temp_d = re.search("WQ([0-9]*)-F.*",dad)
		print dad
		print temp_d
		print type(temp_d)
		temp_m = re.search(".*-(M.*)", mom)
		temp_s = re.search(".*-(S.*)",preg)
		print type(temp_m)
		name = dad + "-" + temp_m.group(1) + "-" + temp_s.group(1)

		# "name": family_no.group(1)
		insert_data = {
			"member_id": self.bind_object.sheet.member_id,
			"dad_id": dad,
			"mom_id": mom,
			"preg_id": preg,
			"created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
			"family_id": temp_d.group(1),
			"status":'start',
			"name": name
		}
		try:
			collection = self.database['sg_father']
			father_id = collection.insert_one(insert_data).inserted_id
		except Exception as e:
			self.bind_object.logger.error('导入家系主表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入家系主表成功")
		return father_id



	# "status": "end",
	# @report_check
	def add_pt_father(self, father_id, err_min, dedup):
		params = dict()
		params['err_min'] = err_min
		params['dedup'] = dedup
		name = 'err-' + str(err_min) + '_dedup-'+ str(dedup)
		insert_data = {
			"father_id": father_id,
			"name": name,
			"created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
		}

		collection = self.database['sg_pt_father']
		new_params = param_pack(params)
		insert_data["params"] = new_params
		# collection.insert_data["params"] = params
		try:
			pt_father_id = collection.insert_one(insert_data).inserted_id
			# collection.insert_one(insert_data)
		except Exception as e:
			self.bind_object.logger.error('导入交互主表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入交互主表成功")
		return pt_father_id

	@report_check
	def add_sg_ref_file(self,ref_fasta,targets_bedfile,ref_point,fastq_path):
		insert_data={
			"ref_fasta": ref_fasta,
			"targets_bedfile": targets_bedfile,
			"ref_poins": ref_point,
			"fastq_path":fastq_path,
		}
		try:
			collection = self.database['sg_pt_ref_file']
			collection.insert_one(insert_data).inserted_id
		# collection.insert_one(insert_data)
		except Exception as e:
			self.bind_object.logger.error('导入参考文件表出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入参考文件表成功")

	@report_check
	def add_sg_pt_father_detail(self,file_path,pt_father_id):
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "chrom":
					continue
				insert_data = {
					# "task_id": self.bind_object.id,
					"pt_father_id": pt_father_id,
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
				collection = self.database['sg_pt_father_detail']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入调试页面表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入调试页面表格成功")

	@report_check
	def add_pt_father_figure(self, output_dir,pt_father_id):
		fs = gridfs.GridFS(self.database)
		family_fig = fs.put(open(output_dir + '/family.png', 'r'))
		figure1 = fs.put(open(output_dir + '/fig1.png', 'r'))
		figure2 = fs.put(open(output_dir + '/fig2.png', 'r'))
		preg_percent = fs.put(open(output_dir + '/preg_percent.png', 'r'))
		update_data = {
			# "task_id": self.bind_object.id,
			"pt_father_id": pt_father_id,
			'family_fig': family_fig,
			'figure1': figure1,
			'figure2': figure2,
			'preg_percent': preg_percent
		}
		try:
			collection = self.database['sg_pt_father_figure']
			figure_id = collection.insert_one(update_data).inserted_id
		except Exception as e:
			self.bind_object.logger.error('导入图片表格出错：{}'.format(e))
		else:
			self.bind_object.logger.info("导入图片表格成功")
		return figure_id

	@report_check
	def add_analysis_tab(self, file_path,pt_father_id):
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "dad.id":
					continue
				temp_fp = eval(line[4])
				RCP = temp_fp / (temp_fp + 1)
				insert_data = {
					# "task_id": self.bind_object.id,
					"pt_father_id": pt_father_id,
					"dad_id": line[0],
					"test_pos_n": line[1],
					"err_pos_n": line[2],
					"err_rate": line[3],
					"fq": line[4],
					"dp": line[5],
					"eff_rate": line[6],
					"ineff_rate": line[7],
					"result": line[8],
					"rcp": float(RCP)
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_father_analysis']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入是否匹配表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入是否匹配表格成功")

	@report_check
	def add_info_detail(self, file_path,pt_father_id):
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "bed.preg.id":
					continue
				insert_data = {
					# "task_id": self.bind_object.id,
					"pt_father_id": pt_father_id,
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
				collection = self.database['sg_pt_father_result_info']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入基本信息表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入基本信息表格成功")

	# @report_check
	def add_test_pos(self, file_path, pt_father_id):
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				if line[0] == "检测位点编号":
					continue
				insert_data = {
					# "task_id": self.bind_object.id,
					"pt_father_id": pt_father_id,
					"test_no": line[0],
					"chrom": line[1],
					"dad_geno": line[2],
					"mom_geno": line[3],
					"preg_geno": line[4],
					"is_mis": line[5]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_father_test_pos']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入位点信息表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入位点信息表格成功")
