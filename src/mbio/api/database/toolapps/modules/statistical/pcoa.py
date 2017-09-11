# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config


class Pcoa(Base):
    def __init__(self, bind_object):
        super(Pcoa, self).__init__(bind_object)
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
        if self.bind_object._task.option("group_table").is_set:
            group_detail = self.get_group_detail(self.bind_object._task.option("group_table").prop["path"])
            self.bind_object.logger.info(group_detail)
        else:
            group_detail = None
        self.main_id = self.pcoa_in(group_detail)
        self.table_ids = self.table_in(group_detail)

    def get_group_detail(self, group_table):
        """
        根据分组文件得到具体的分组方案
        """
        group_samples = {}  # 分组对应中新样本对应的旧样本
        with open(group_table, "r") as f:
            line = f.readline().rstrip()
            line = re.split("\t", line)
            if line[1] == "##empty_group##":
                is_empty = True
            else:
                is_empty = False
            for i in range(1, len(line)):
                group_samples[line[i]] = []
            for item in f:
                item = item.rstrip().split("\t")
                for i in range(1, len(line)):
                    if item[i]:
                        group_samples[line[i]].append(item[0])
                    else:
                        self.bind_object.logger.info("{}样本不在分组方案{}内".format(item[0], line[i]))
        return group_samples

    def table_in(self, group_detail=None):
        """
        导入主表信息
        """
        ratation1 = self.insert_table(self.output_dir + '/Pcoa/pcoa_sites.xls', '样本坐标表',
                                     '样本坐标表', group_detail)
        ratation2 = self.insert_table(self.output_dir + '/Pcoa/pcoa_eigenvalues.xls', '矩阵特征值表',
                                      '矩阵特征值', group_detail)
        ratation3 = self.insert_table(self.output_dir + '/Pcoa/pcoa_eigenvaluespre.xls', '主成分解释度表',
                                      '主成分解释度表', group_detail)
        return [ratation1, ratation2, ratation3]

    def insert_table(self, fp, name, desc, group_detail=None):
        self.bind_object.logger.info('开始导入{}表'.format(fp))
        with open(fp) as f:
            line = 'ID' + f.readline()
            columns = line.strip().split('\t')
            insert_data = []
            table_id = self.db['table'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name=name,
                attrs=columns,
                desc=desc,
                status='end',
                group_detail=group_detail,
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )).inserted_id
            for line in f:
                if line.startswith('\t'):
                    line = 'ID' + line
                line_split = line.strip().split('\t')
                if fp == self.output_dir + '/Pcoa/pcoa_sites.xls':
                    self.sample_list.append(line_split[0])
                data = dict(zip(columns, line_split))
                data['table_id'] = table_id
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)
            self.bind_object.logger.info('table表导入结束')
            if fp == self.output_dir + '/Pcoa/pcoa_sites.xls':
                self.bind_object.logger.info('开始导入样本id')
                self.insert_specimens(self.sample_list)
        return table_id

    def pcoa_in(self, group_detail=None):
        """
        导入pcoa相关信息
        """
        self.bind_object.logger.info('主表导入')
        pcoa_id = self.db['pcoa'].insert_one(SON(
            project_sn=self.bind_object.sheet.project_sn,
            task_id=self.bind_object.id,
            name='pcoa',
            desc='pcoa分析',
            status='end',
            group_detail=group_detail,
            created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )).inserted_id
        self.bind_object.logger.info('主表导入结束')
        with open(self.output_dir + '/Pcoa/pcoa_sites.xls', 'r') as s:
            self.bind_object.logger.info("pcoa画图数据表导入开始")
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
                otu_detail['pcoa_id'] = pcoa_id
                otu_detail['row_name'] = line[0]
                r_list.append(line[0])  # 后续画图做准备
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = float(sample_num[i])  # 保证画图时取到的数据是数值型
                insert_data.append(otu_detail)
            try:
                self.db['pcoa_detail'].insert_many(insert_data)
            except Exception as e:
                self.bind_object.logger.info("pcoa画图数据导入出错{}".format(e))
                raise Exception("pcoa画图数据导入出错{}".format(e))
            else:
                self.bind_object.logger.info("pcoa画图数据表导入完成")
        return pcoa_id

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
