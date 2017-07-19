# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config


class Nmds(Base):
    def __init__(self, bind_object):
        super(Nmds, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        self.work_dir = self.bind_object.work_dir
        if Config().MONGODB == 'sanger':
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()
        self.sample_list = []

    @report_check
    def run(self):
        """
        运行函数
        """
        self.main_id = self.nmds_in()
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def table_in(self):
        """
		导入表格相关信息
		"""
        ratation = self.insert_table(self.output_dir + '/Nmds/nmds_sites.xls', 'nmds分析结果表',
                                     '画图的原始数据表')
        return [ratation]

    def insert_table(self, fp, name, desc):
        self.bind_object.logger.info('开始导入table表')
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
                self.sample_list.append(line_split[0])
                data = dict(zip(columns, line_split))
                data['table_id'] = table_id
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)
            self.bind_object.logger.info('table表导入结束')
            self.bind_object.logger.info('开始导入样本id')
            self.insert_specimens(self.sample_list)
        return table_id

    def nmds_in(self):
        """
        导入nmds相关信息
        """
        with open(self.output_dir + '/Nmds/nmds_stress.xls') as f:
            self.bind_object.logger.info('主表导入')
            for line in f:
                stress = line.strip('\n')
            nmds_id = self.db['nmds'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='nmds',
                desc='nmds分析',
                status='end',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                stress=stress,
            )).inserted_id
            self.bind_object.logger.info('主表导入结束')
        with open(self.output_dir + '/Nmds/nmds_sites.xls', 'r') as s:
            self.bind_object.logger.info("nmds画图数据表导入开始")
            insert_data = []
            head = s.next().strip('\r\n')  # windows换行符
            head = re.split('\t', head)
            new_head = head[1:]
            r_list = []
            for line in s:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                sample_num = line[1:]
                otu_detail = dict()
                otu_detail['nmds_id'] = nmds_id
                otu_detail['row_name'] = line[0]
                r_list.append(line[0])  # 后续画图做准备
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = float(sample_num[i])  # 保证画图时取到的数据是数值型
                insert_data.append(otu_detail)
            try:
                self.db['nmds_detail'].insert_many(insert_data)
            except Exception as e:
                self.bind_object.logger.info("nmds画图数据导入出错{}".format(e))
                raise Exception("nmds画图数据导入出错{}".format(e))
            else:
                self.bind_object.logger.info("nmds画图数据表导入完成")
        return nmds_id

    def insert_specimens(self, specimen_names):
        """
		"""
        task_id = self.bind_object.id
        project_sn = self.bind_object.sheet.project_sn
        datas = [SON(project_sn=project_sn, task_id=task_id, name=i) for i in specimen_names]
        ids = self.db['specimen'].insert_many(datas).inserted_ids
        self.bind_object.logger.info("样本id导入结束")
        return SON(zip(specimen_names, ids))

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
