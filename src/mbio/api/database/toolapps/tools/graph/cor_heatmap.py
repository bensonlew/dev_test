# -*- coding: utf-8 -*-
# __author__ = 'gaohao'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config
import os


class CorHeatmap(Base):
    def __init__(self, bind_object):
        super(CorHeatmap, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir ##结果目录
        self.work_dir = self.bind_object.work_dir ##项目目录
        if Config().MONGODB == 'sanger':   ##定义小工具的mongo数据库名称
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()

    @report_check
    def run(self):
        """
        运行函数
        """
        self.main_id = self.heatmap_in(self.output_dir)
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def table_in(self):
        """
		导入表格相关信息
		"""
        correlation = self.insert_table(self.output_dir + '/pearsons_correlation.xls', 'cor_heatmap相关性系数结果表',
                                     'cor_heatmap相关性系数数据')
        pvalue = self.insert_table(self.output_dir + '/pearsons_pvalue.xls', 'cor_heatmap的P值结果表',
                                     'cor_heatmap的P值的数据')
        return [correlation,pvalue]

    def insert_table(self, fp, name, desc):
        with open(fp) as f:
            columns = f.readline().strip().split('\t')
            envs=columns[1:]
            insert_data = []
            if 'pearsons_correlation.xls'in fp:
                table_id = self.db['table'].insert_one(SON(
                    project_sn=self.bind_object.sheet.project_sn,
                    task_id=self.bind_object.id,
                    name=name,
                    attrs=columns,
                    desc=desc,
                    status='end',
                    created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )).inserted_id
                for line in f:
                    line = line.strip().split("\t")
                    data = {
                        "cor_heatmap_id": table_id,
                        "species_name": line[0],
                        "value_type": "pearsons_cor"
                    }
                    for n, e in enumerate(envs):
                        data[e] = line[n + 1]
                    insert_data.append(data)
                self.db['table_detail'].insert_many(insert_data)
            else:
                table_id = self.db['table'].insert_one(SON(
                    project_sn=self.bind_object.sheet.project_sn,
                    task_id=self.bind_object.id,
                    name=name,
                    attrs=columns,
                    desc=desc,
                    status='end',
                    created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

                )).inserted_id
                for line in f:
                    line = line.strip().split("\t")
                    data = {
                        "cor_heatmap_id": table_id,
                        "species_name": line[0],
                        "value_type": "pearsons_pva"
                    }
                    for n, e in enumerate(envs):
                        data[e] = line[n + 1]
                    insert_data.append(data)
                self.db['table_detail'].insert_many(insert_data)
        return table_id

    def heatmap_in(self,dir):
        """
        导入heatmap图相关信息
        """
        files =os.listdir(dir)
        for file in files:
            if file == 'pearsons_correlation.xls':
                with open (dir+"/"+file,'r') as f:
                    columns = f.readline().strip().split('\t')
                    insert_data = []
                    self.bind_object.logger.info("cor_heatmap主表correlation写入")
                    cor_heatmap_cor_id= self.db['cor_heatmap'].insert_one(SON(
                        project_sn=self.bind_object.sheet.project_sn,
                        task_id=self.bind_object.id,
                        name='pearsons_correlation',
                        desc='相关性系数聚类热图值',
                        status='failed',
                        created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )).inserted_id
                    self.bind_object.logger.info("cor_heatmap主表pearsons_cor写入结束")
                    envs = columns[1:]
                    r_list = []
                    for line in f:
                        line = line.strip().split("\t")
                        data = {
                           "cor_heatmap_id": cor_heatmap_cor_id,
                           "species_name": line[0],
                           "value_type": "pearsons_cor"
                        }
                        r_list.append(line[0])
                        for n, e in enumerate(envs):
                           data[e] = line[n + 1]
                        insert_data.append(data)
                    self.bind_object.logger.info("cor_heatmap表格数据导入开始")
                try:
                    self.db['cor_heatmap_detail'].insert_many(insert_data)
                except Exception as e:
                    self.bind_object.logger.info("cor_heatmap数据表导入出错{}".format(e))
                    raise Exception("cor_heatmap数据表导入出错{}".format(e))
                else:
                    self.bind_object.logger.info("cor_heatmap数据表导入完成")
            if file == 'pearsons_pvalue.xls':
                with open (dir+"/"+file,'r') as f:
                    columns = f.readline().strip().split('\t')
                    insert_data = []
                    self.bind_object.logger.info("cor_heatmap主表pvalue写入")
                    cor_heatmap_id = self.db['cor_heatmap'].insert_one(SON(
                        project_sn=self.bind_object.sheet.project_sn,
                        task_id=self.bind_object.id,
                        name='pearsons_pvalue',
                        desc='相关性聚类热图p值',
                        status='end',
                        created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )).inserted_id
                    self.bind_object.logger.info("cor_heatmap主表pvalue写入结束")
                    for line in f:
                        line = line.strip().split("\t")
                        data = {
                           "cor_heatmap_id": cor_heatmap_id,
                           "species_name": line[0],
                           "value_type": "pearsons_pva"
                        }
                        for n, e in enumerate(envs):
                           data[e] = line[n + 1]
                        insert_data.append(data)
                    self.bind_object.logger.info("cor_heatmap表格数据导入开始")
                try:
                    self.db['cor_heatmap_detail'].insert_many(insert_data)
                except Exception as e:
                    self.bind_object.logger.info("cor_heatmap数据表导入出错{}".format(e))
                    raise Exception("cor_heatmap数据表导入出错{}".format(e))
                else:
                    self.bind_object.logger.info("cor_heatmap数据表导入完成")
        row_tree_path = self.output_dir + '/row_tre'
        if os.path.exists(row_tree_path):
            self.bind_object.logger.info("拥有行聚类树")
            with open(row_tree_path, "r") as m:
                row_tree = m.readline().strip()
                raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', row_tree)
                row_list = [i[1] for i in raw_samp]
        else:
            row_tree = ""
            row_list = r_list
            # row_list = []
        col_tree_path = self.output_dir + '/col_tre'
        if os.path.exists(col_tree_path):
            self.bind_object.logger.info("拥有列聚类树")
            with open(col_tree_path, "r") as n:
                col_tree = n.readline().strip()
                raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', col_tree)
                col_list = [i[1] for i in raw_samp]
        else:
            col_tree = ""
            col_list = envs
            # col_list = []
        self.bind_object.logger.info("cor_heatmap_cor_id：{}".format(cor_heatmap_cor_id))
        try:
            self.db['cor_heatmap'].update_one({'_id': cor_heatmap_cor_id}, {'$set':
                                                            {'status': 'end',
                                                            'row_tree': row_tree,
                                                            'row_list': row_list,
                                                            'col_tree': col_tree,
                                                            'col_list': col_list}})
        except Exception as e:
            self.bind_object.logger.info("cor_heatmap主表更新出错{}".format(e))
            raise Exception("cor_heatmap主表更新出错{}".format(e))
        else:
            self.bind_object.logger.info("cor_heatmap主表更新完成")
        return cor_heatmap_cor_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
