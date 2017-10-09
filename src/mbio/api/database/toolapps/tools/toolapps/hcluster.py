# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config


class Hcluster(Base):
    def __init__(self, bind_object):
        super(Hcluster, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        self.work_dir = self.bind_object.work_dir
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
        self.main_id = self.hcluster_in()
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def table_in(self):
        """
		导入表格相关信息
		"""
        ratation = self.insert_table(self.output_dir + '/data_table', '聚类树原始数据表',
                                     '生成聚类树的原始数据表')
        return [ratation]

    def insert_table(self, fp, name, desc):
        with open(fp) as f:
            columns = f.readline().strip().split('\t')
            insert_data = []
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

    def hcluster_in(self):
        """
        导入venn图相关信息
        """
        with open(self.output_dir + '/hcluster.tre') as f:
            hcluster_id = self.db['tree'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='hcluster',
                desc='层次聚类树图',
                status='end',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            line = f.readline()
            tree = line.strip()
            raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', tree)
            sample_list = [i[1] for i in raw_samp]
            self.bind_object.logger.info(tree)
            specimen_ids_dict = self.insert_specimens(sample_list)
            try:
                collection = self.db["tree"]
                collection.update_one({"_id": hcluster_id}, {"$set": {"value": line}})
            except Exception, e:
                self.bind_object.logger.error("导入tree信息出错:%s" % e)
                raise Exception("导入tree信息出错:%s" % e)
            else:
                self.bind_object.logger.info("导入tree信息成功!")
            # reverse_ids = SON([(str(n), m) for m, n in specimen_ids_dict.iteritems()])  # add by zhouxuan 20170508
            # self.db['specimen_group'].insert_one(SON(data=[('project_sn', self.bind_object.sheet.project_sn),
            #                                                ('task_id', self.bind_object.id),
            #                                                ('visual_type', ['hcluster']),
            #                                                ('visual_id', [hcluster_id]),
            #                                                ('name', 'ALL'),
            #                                                ('category_names', ['ALL']),
            #                                                ('specimen', [reverse_ids])]))
        return hcluster_id

    def insert_specimens(self, specimen_names):  # add by zhouxuan 20170508
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
