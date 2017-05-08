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
        return self.main_id
        pass



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
            try:
                collection = self.db["tree"]
                collection.update_one({"_id": hcluster_id}, {"$set": {"value": line}})
            except Exception, e:
                self.bind_object.logger.error("导入tree信息出错:%s" % e)
            else:
                self.bind_object.logger.info("导入tree信息成功!")
        with open(self.output_dir + "/data_table") as m:
            self.bind_object.logger.info("tree的输入表导入开始")
            insert_data = []
            head = m.next().strip('\r\n')  # windows换行符
            head = re.split('\t', head)
            new_head = head[1:]
            for line in m:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                sample_num = line[1:]
                otu_detail = dict()
                otu_detail['hcluster_id'] = hcluster_id
                otu_detail['row_name'] = line[0]
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = sample_num[i]
                insert_data.append(otu_detail)
            self.bind_object.logger.info("tree的输入表导入结束")
            try:
                self.db['tree_detail'].insert_many(insert_data)
            except Exception as e:
                self.bind_object.logger.info("tree的输入表导入出错{}".format(e))
            else:
                self.bind_object.logger.info("tree的输入表导入完成")
        return hcluster_id

    def check(self):
        """
        检查文件格式是否正确
        """
        pass
