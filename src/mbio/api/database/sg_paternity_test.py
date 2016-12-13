# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

from biocluster.api.database.base import Base, report_check
import os
from biocluster.config import Config
from pymongo import MongoClient

class SgPaternityTest(Base):
	'''
	将前端需要调用的文件导入mongo数据库
	'''
	def __init__(self, bind_object):
		super(SgPaternityTest, self).__init__(bind_object)
		# self._db_name = Config().MONGODB
		self.mongo_client = MongoClient(Config().MONGO_URI)
		self.database = self.mongo_client['tsanger_paternity_test']

	@report_check
	def add_sg_pt_family_detail(self,file_path):
		self.bind_object.logger.info("开始导入tab表")
		sg_pt_family_detail = list()
		with open(file_path, 'r') as f:
			for line in f:
				line = line.strip()
				line = line.split('\t')
				insert_data = {
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
					"mj_gene": line[41],
					"is_test": line[42],
					"is_mis": line[43],
					"mustbe": line[44],
					"mustnotbe": line[45],
					"good": line[46],
					"pi": line[47]
				}
				sg_pt_family_detail.append(insert_data)
			try:
				collection = self.database['sg_pt_family_detail']
				collection.insert_many(sg_pt_family_detail)
			except Exception as e:
				self.bind_object.logger.error('导入tab表格出错：{}'.format(e))
			else:
				self.bind_object.logger.info("导入tab表格成功")


