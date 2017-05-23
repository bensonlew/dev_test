# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config
import os


class HcHeatmap(Base):
    def __init__(self, bind_object):
        super(HcHeatmap, self).__init__(bind_object)
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
        self.main_id = self.heatmap_in()
        return self.main_id
        pass

    def heatmap_in(self):
        """
        导入heatmap图相关信息
        """
        with open(self.output_dir + '/result_data') as f:
            self.bind_object.logger.info("heatmap主表写入")
            heatmap_id = self.db['heatmap'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='heatmap',
                desc='聚类热图',
                status='failed',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            self.bind_object.logger.info("heatmap主表写入结束")
            insert_data = []
            head = f.next().strip('\r\n')  # windows换行符
            head = re.split('\t', head)
            new_head = head[1:]
            for line in f:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                sample_num = line[1:]
                otu_detail = dict()
                otu_detail['heatmap_id'] = heatmap_id
                otu_detail['row_name'] = line[0]
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = sample_num[i]
                insert_data.append(otu_detail)
            self.bind_object.logger.info("heatmap表格数据导入开始")
            try:
                self.db['heatmap_detail'].insert_many(insert_data)
            except Exception as e:
                self.bind_object.logger.info("heatmap数据表导入出错{}".format(e))
            else:
                self.bind_object.logger.info("heatmap数据表导入完成")
            row_tree_path = self.output_dir + '/row_tre'
            if os.path.exists(row_tree_path):
                self.bind_object.logger.info("拥有行聚类树")
                with open(row_tree_path, "r") as m:
                    row_tree = m.readline().strip()
                    raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', row_tree)
                    row_list = [i[1] for i in raw_samp]
            else:
                row_tree = ""
                row_list = []
            col_tree_path = self.output_dir + '/col_tre'
            if os.path.exists(col_tree_path):
                self.bind_object.logger.info("拥有列聚类树")
                with open(col_tree_path, "r") as n:
                    col_tree = n.readline().strip()
                    raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', col_tree)
                    col_list = [i[1] for i in raw_samp]
            else:
                col_tree = ""
                col_list = []
            self.bind_object.logger.info("heatmap_id：{}".format(heatmap_id))
            try:
                self.db['heatmap'].update_one({'_id': heatmap_id}, {'$set':
                                                            {'status': 'end',
                                                            'row_tree': row_tree,
                                                            'row_list': row_list,
                                                            'col_tree': col_tree,
                                                            'col_list': col_list}})
            except Exception as e:
                self.bind_object.logger.info("heatmap主表更新出错{}".format(e))
            else:
                self.bind_object.logger.info("heatmap主表更新完成")
            return heatmap_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
