# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
# last_modify:20170926
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class HclusterTree(Base):
    def __init__(self, bind_object):
        super(HclusterTree, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_hcluster_tree(self, file_path, specimen_graphic, geneset_id, anno_id, level_id ):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(file_path, 'rb') as f:
            line = f.readlines()
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': '',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_id': geneset_id,
            'anno_id': anno_id,
            'specimen_graphic': specimen_graphic,
            'level_id': level_id,
            'specimen_tree': line
        }
        collection = self.db['hcluster_tree']
        # 将主表名称写在这里
        hcluster_tree_id = collection.insert_one(insert_data).inserted_id
        return hcluster_tree_id

    @report_check
    def add_specimen_distance(self,geneset_id, anno_id, level_id,hcluster_tree_id ):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': '',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_id': geneset_id,
            'anno_id': anno_id,
            'level_id': level_id,
            'hcluster_tree_id': hcluster_tree_id
        }
        collection = self.db['specimen_distance']
        # 将主表名称写在这里
        specimen_distance_id = collection.insert_one(insert_data).inserted_id
        return specimen_distance_id

    @report_check
    def add_specimen_distance_detail(self,specimen_distance_id,file_path ):
        if not isinstance(specimen_distance_id, ObjectId):  # 检查传入的geneset_id是否符合ObjectId类型
            if isinstance(specimen_distance_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                specimen_distance_id = ObjectId(specimen_distance_id)
            else:  # 如果是其他类型，则报错
                raise Exception('specimen_distance_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(file_path):
            raise Exception('file_path所指定的路径不存在，请检查！')
        data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        with open(file_path, 'rb') as f:
            lines = f.readlines()
            line0 = lines[0].strip().split('\t')
            sample = line0[0:]
            for line in lines[1:]:
                data = [('specimen_distance_id', specimen_distance_id)]
                line = line.strip().split('\t')
                data.append(('specimen_name', line[0]))
                i = 1
                for eachsample in sample:
                    data.append((eachsample, line[i]))
                    i += 1
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['specimen_distance_detail']
            # 将detail表名称写在这里
            collection.insert_many(data_list)  # 用insert_many批量导入数据库，insert_one一次只能导入一条记录
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
