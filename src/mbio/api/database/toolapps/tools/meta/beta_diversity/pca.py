# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config


class Pca(Base):
    """
    """
    def __init__(self, bind_object):
        super(Pca, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        if Config().MONGODB == 'sanger':
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()

    @report_check
    def run(self):
        """
        运行函数
        """
        self.main_id = self.scatter_in()
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def table_in(self):
        """
        导入表格相关信息
        """
        ratation = self.insert_table(self.output_dir + '/pca_rotation.xls', 'PCA_特征主成分贡献度表',
                                     'PCA分析中样本特征的贡献度统计结果，例如在OTU表的PCA分析中代表物种/OTU的贡献度')
        importance = self.insert_table(self.output_dir + '/pca_importance.xls', 'PCA_解释度表', 'PCA结果坐标轴的解释度值')
        return [ratation, importance]

    def insert_table(self, fp, name, desc):
        with open(fp) as f:
            columns = f.readline().strip().split('\t')
            insert_data = []
            if fp == self.output_dir + '/pca_importance.xls':
                table_id = self.db['table'].insert_one(SON(
                    project_sn=self.bind_object.sheet.project_sn,
                    task_id=self.bind_object.id,
                    name=name,
                    attrs=columns,
                    desc=desc,
                    status='end',
                    created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    type="pca_im",
                )).inserted_id
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
                line_split = line.strip().split('\t')
                data = dict(zip(columns, line_split))
                data['table_id'] = table_id
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)
        return table_id

    def scatter_in(self):
        """
        导入散点图相关信息
        """
        with open(self.output_dir + '/pca_sites.xls') as f:
            attrs = f.readline().strip().split('\t')[1:]
            samples = []
            insert_data = []
            scatter_id = self.db['scatter'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='pca',
                attrs=attrs,
                desc='PCA主成分分析',
                status='faild',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            for line in f:
                line_split = line.strip().split('\t')
                samples.append(line_split[0])
                data = SON(specimen_name=line_split[0],
                           scatter_id=scatter_id)
                data.update(zip(attrs, line_split[1:]))
                insert_data.append(data)
            specimen_ids_dict = self.insert_specimens(samples)
            for i in insert_data:
                i['specimen_id'] = specimen_ids_dict[i['specimen_name']]
            self.db['scatter_detail'].insert_many(insert_data)
            self.db['scatter'].update_one({'_id': scatter_id}, {'$set': {'status': 'end', 'specimen_ids': specimen_ids_dict.values()}})
            return scatter_id

    def insert_specimens(self, specimen_names):
        """
        """
        task_id = self.bind_object.id
        project_sn = self.bind_object.sheet.project_sn
        datas = [SON(project_sn=project_sn, task_id=task_id, name=i) for i in specimen_names]
        ids = self.db['specimen'].insert_many(datas).inserted_ids
        return SON(zip(specimen_names, ids))



    def check(self):
        """
        检查文件格式是否正确
        """
        pass

