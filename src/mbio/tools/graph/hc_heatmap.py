# -*- coding: utf-8 -*-
# __author__ = zhouxuan
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import re
import collections

class HcHeatmapAgent(Agent):
	"""
	小工具聚类heatmap图：实现任意二维数据的热图并含有行和列的聚类树
	auther: xuan.zhou
	last_modified: 20170417
	使用脚本实现聚类树
	"""
	def __init__(self, parent):
		super(HcHeatmapAgent, self).__init__(parent)
		options = [
			{"name": "data_table", "type": "infile", "format": "meta.otu.otu_table"},  # 数据表
			{"name": "row_method", "type": "string", "default": ""},  # 行聚类方式
			{"name": "col_method", "type": "string", "default": ""},  # 列聚类方式
			{"name": "col_number", "type": "string", "default": "10"},  # 列数
			{"name": "row_number", "type": "string", "default": "10"}  # 行数
        ]
		self.add_option(options)
		self.step.add_steps('hc_heatmap')
		self.on('start', self.step_start)
		self.on('end', self.step_end)
		self.new_data = ''

	def step_start(self):
		self.step.hc_heatmap.start()
		self.step.update()

	def step_end(self):
		self.step.hc_heatmap.finish()
		self.step.update()

	def check_options(self):
		"""
		参数检测
		:return:
		"""
		if not self.option("data_table"):
			raise OptionError("参数data_table不能为空")
		if self.option("row_method") not in ['average', 'complete', 'single', '']:
			raise OptionError("请选择正确行聚类方式")
		if self.option("col_method") not in ['average', 'complete', 'single', '']:
			raise OptionError("请选择正确列聚类方式")

	def set_resource(self):
		"""
		设置所需资源
		"""
		self._cpu = 10
		self._memory = '10G'

class HcHeatmapTool(Tool):
	"""
	使用脚本/mnt/ilustre/users/sanger-dev/app/bioinfo/statistical/scripts/plot-hcluster_tree_app.pl
	"""
	def __init__(self, config):
		super(HcHeatmapTool, self).__init__(config)
		self.R_path = 'program/R-3.3.1/bin/Rscript'
		self.app_path = 'bioinfo/statistical/scripts/plot-hcluster_tree_app.pl'
		#self.app_path = self.config.SOFTWARE_DIR + '/bioinfo/statistical/scripts/plot-hcluster_tree_app.pl'
		self._version = 1.0

	def get_new_data(self, old_path, new_path, col_number, row_number):
		"""
		使用pandas包完成对于二维表格（行列名称均存在）的筛选，筛选出丰度为前多少的行和列的数据
		:return: 新的表格
		"""
		import pandas as pd
		data = pd.read_table(old_path, header=0)  # 把数据读取成data_frame格式
		col = data.sum(axis=1)  # 计算每行的和
		data = data.join(col.to_frame(name="col"))  # 把每行和加到数据框的最后一列
		data = data.sort(["col"], ascending=False)  # 根据行和排序（由高到低）
		data_1 = data.drop(["col"], axis=1)  # 从数据框中去除行和
		if row_number != "":
			data_1 = data_1.iloc[:int(row_number)]  # 筛选出前多少行
		name = data_1.T.iloc[0].to_frame(name="name")  # 原数据行名的保留
		row = data_1.T.iloc[1:].sum(axis=1)  # 去除行名数据框转置求行和也就是求原数据的列和
		data_2 = data_1.T.iloc[1:].join(row.to_frame(name="row"))  # 以下四行就是以相同的方式筛选出前多少行(由于转置所以实际是列)
		data_2 = data_2.sort(["row"], ascending=False)
		data_2 = data_2.drop(["row"], axis=1)
		if col_number != "":
			data_2 = data_2.iloc[:int(col_number)]
		data_ = name.join(data_2.T)  # 恢复行名
		data_.to_csv(new_path, sep="\t", index=False)  # 数据框写入文件

	def create_tree(self):
		"""
        plot-hcluster_tree_app.pl,输出画图所需的树文件
        """
		if self.option("col_number") == "" and self.option("row_number") == "":
			self.new_data = self.option('data_table').prop['path']
		else:
			self.new_data = os.path.join(self.work_dir, "new_data.xls")
			self.get_new_data(old_path=self.option('data_table').prop['path'], new_path=self.new_data,
		                  col_number=self.option("col_number"), row_number=self.option("row_number"))
		if self.option('row_method') == '' and self.option('col_method') == '':
			self.set_output()
		else:  # -m ~ col
			if self.option('row_method') != '' and self.option('col_method') == '':
				cmd = '{} -i {} -m_1 {} -trans row -o {}'.format(self.app_path,
				                                                 self.new_data,
				                                                self.option('row_method'),
				                                                self.output_dir)
			elif self.option('row_method') == '' and self.option('col_method') != '':
				cmd = '{} -i {} -m {} -trans col -o {}'.format(self.app_path,
				                                               self.new_data,
				                                               self.option('col_method'),
				                                               self.output_dir)
			else:
				cmd = '{} -i {} -m {} -m_1 {} -trans both -o {}'.format(self.app_path,
				                                                        self.new_data,
				                                                        self.option('col_method'),
				                                                        self.option('row_method'),
				                                                        self.output_dir)
			self.logger.info("开始运行plot-hcluster_tree_app.pl")
			command = self.add_command("hc_heatmap", cmd)
			command.run()
			self.wait(command)
			if command.return_code == 0:
				self.logger.info("运行plot-hcluster_tree_app.pl完成，正确生成hc.cmd.r脚本")
			else:
				self.set_error("运行plot-hcluster_tree_app.pl运行出错!")
				raise Exception("运行plot-hcluster_tree_app.pl运行出错，请检查输入的参数是否正确")
			r_cmd = '{} {}/hc.cmd.r'.format(self.R_path, self.work_dir)
			self.logger.info("运行hc.cmd.r")
			r_command = self.add_command("hc_r", r_cmd)
			r_command.run()
			self.wait(r_command)
			if r_command.return_code == 0:
				self.logger.info("运行hc.cmd.r脚本成功")
			else:
				self.set_error("运行hc.cmd.r运行出错!")
				raise Exception("运行hc.cmd.r运行出错，请检查输入的参数是否正确")
			self.set_output()

	def set_output(self):
		os.link(self.new_data, os.path.join(self.output_dir, "result_data"))
		file_name = os.listdir(self.output_dir)
		if self.option('row_method') != '':
			for name in file_name:
				if re.search('(\.tre)$', name):
					os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "col_tre"))
				if re.search('(\.ttre)$', name):
					os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "row_tre"))
			self.logger.info("存在行聚类树")
		else:
			for name in file_name:
				if re.search('(\.tre)$', name):
					os.link(os.path.join(self.output_dir, name), os.path.join(self.output_dir, "col_tre"))
			self.logger.info("存在列聚类树")

	def run(self):
		"""
		运行
		"""
		super(HcHeatmapTool, self).run()
		self.create_tree()
		self.end()