# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import json
from biocluster.api.database.base import Base, report_check
import re
import os
import datetime
from bson import SON
from biocluster.config import Config


class SimpleBar(Base):
    def __init__(self, bind_object):
        super(SimpleBar, self).__init__(bind_object)
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
        self.main_id = self.simple_bar_in()
        return self.main_id
        pass

    def simple_bar_in(self):
        """
        导入simple_bar图相关信息
        """

        all_file = os.listdir(self.output_dir)
        for fr in all_file:
            if fr.endswith("matrix_bar.xls"):
                files = os.path.join(self.output_dir, fr)
                with open(files) as f:
                    simple_bar_id = self.db['bar'].insert_one(SON(
                        project_sn=self.bind_object.sheet.project_sn,
                        task_id=self.bind_object.id,
                        name='bar',
                        desc='一个样本多个值的柱形图表格',
                        status='faild',
                        created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )).inserted_id
                    lines = f.readlines()
                    lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                    lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                    first_line = lines[0].strip().split("\t")
                    xAxis = []
                    for i in first_line[1:]:
                        xAxis.append(i)
                    samples = []
                    insert_data = []
                    for line in lines[1:]:
                        sample_data = []
                        line_split = line.strip().split("\t")
                        samples.append(line_split[0])
                        for i in line_split[1:]:
                            if i != "":
                                sample_data.append(float(i))
                        data = SON(sample_name=line_split[0], bar_id=simple_bar_id, value=sample_data)
                        insert_data.append(data)
                    self.db['bar_detail'].insert_many(insert_data)
                    self.db['bar'].update_one({'_id': simple_bar_id},
                                                {'$set': {'status': 'end', 'attrs': samples, 'categories': xAxis}})
                    return simple_bar_id
            if fr.endswith("matrix_pie.xls"):
                files = os.path.join(self.output_dir, fr)
                with open(files) as f:
                    simple_pie_id = self.db['pie'].insert_one(SON(
                        project_sn=self.bind_object.sheet.project_sn,
                        task_id=self.bind_object.id,
                        name='pie',
                        desc='一个样本多个值的饼图表格',
                        status='faild',
                        created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )).inserted_id
                    lines = f.readlines()
                    lines = [line for line in lines if (line != "\r\n") and (line != "\n")]
                    lines = [line for line in lines if not re.search(r"^(\s*\t+?)\s*\t*\n*", line)]
                    first_line = lines[0].strip().split("\t")
                    xAxis = []
                    for i in first_line[1:]:
                        xAxis.append(i)
                    samples = []
                    insert_data = []
                    for line in lines[1:]:
                        sample_data = []
                        line_split = line.strip().split("\t")
                        samples.append(line_split[0])
                        for i in range(len(first_line)-1):
                                dic = dict()
                                dic["name"] = first_line[i+1]
                                dic["value"] = float(line_split[i+1])
                                print dic
                                sample_data.append(dic)
                        data = SON(sample_name=line_split[0], pie_id=simple_pie_id, value=sample_data)
                        insert_data.append(data)
                    self.db['pie_detail'].insert_many(insert_data)
                    self.db['pie'].update_one({'_id': simple_pie_id},
                                                {'$set': {'status': 'end', 'attrs': samples, 'categories': xAxis}})
                    return simple_pie_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
