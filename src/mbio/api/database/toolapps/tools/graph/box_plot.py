# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import json
from biocluster.api.database.base import Base, report_check
import re
import os
import datetime
from bson import SON
from biocluster.config import Config


class BoxPlot(Base):
    def __init__(self, bind_object):
        super(BoxPlot, self).__init__(bind_object)
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
        self.main_id = self.box_plot_in()
        self.table_ids = self.table_in()
        return self.main_id
        pass

    def box_plot_in(self):
        """
        导入box_plot图相关信息
        """
        self.bind_object.logger.info("开始箱线图导表")
        box_file = self.output_dir + '/box_data.xls'
        with open(box_file) as f:
            box_plot_id = self.db['box_plot'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='box_plot',
                desc='箱线图的数据',
                status='faild',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            lines = f.readlines()
            lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
            lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
            samples = []
            insert_data = []
            for line in lines[1:]:
                insert_filter = []
                line_split = line.strip().split("\t")
                samples.append(line_split[0])
                if len(line_split) == 6:
                    pass
                else:
                    insert_filter.append(float(line_split[6]))
                data_list = [float(line_split[1]), float(line_split[2]), float(line_split[3]), float(line_split[4]), float(line_split[5]), insert_filter]
                data = SON(sample_name=line_split[0], box_id=box_plot_id, box_data=data_list)
                insert_data.append(data)
            self.db['box_plot_detail'].insert_many(insert_data)
            self.db['box_plot'].update_one({'_id': box_plot_id}, {'$set': {'status': 'end', 'attrs': samples}})
            return box_plot_id

    def table_in(self):
        """
        导入表格相关信息
        """
        self.bind_object.logger.info("开始表格导表")
        value_table = self.insert_table(self.output_dir + '/box_data.xls', '箱线图数据表', '箱线图数据表格')
        return [value_table]

    def insert_table(self, fp, name, desc):
        with open(fp) as f:
            lines = f.readlines()
            columns = lines[0].strip().split("\t")
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
        with open(fp) as f:
            lines = f.readlines()
            columns = lines[0].strip().split("\t")
            for line2 in lines[1:]:
                line_split = line2.strip().split('\t')
                data = dict(zip(columns, line_split))
                data['table_id'] = table_id
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)

        return table_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
