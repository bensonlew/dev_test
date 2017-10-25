# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
from biocluster.api.database.base import Base, report_check
import re
import json
import datetime
from bson.son import SON
# from types import StringTypes
from bson.objectid import ObjectId
from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort
from biocluster.api.database.base import Base, report_check


class DistanceMetagenomic(Base):
    def __init__(self, bind_object):
        super(DistanceMetagenomic, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_dist_table(self, file_path, main=False, level_id=None, dist_id=None, anno_id=None, task_id=None, name=None,
                       params=None, spname_spid=None, geneset_id=None):
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_list = []
        if spname_spid:
            group_detail = {'All': [str(i) for i in spname_spid.values()]}
            params['group_detail'] = group_detail_sort(group_detail)
        # insert main
        if main:
            collection = self.db["geneset"]
            result = collection.find_one({"_id": ObjectId(geneset_id)})
            task_id = result['task_id']
            insert_data = {
                'project_sn': project_sn,
                'task_id': task_id,
                'desc': '',
                'created_ts': created_ts,
                'name': self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else "distance_origin",
                'params': json.dumps(params, sort_keys=True, separators=(',', ':')),
                'status': 'end',
                'geneset_id': geneset_id,
                'anno_id': anno_id,
                'level_id': level_id
            }
            collection = self.db["specimen_distance"]
            dist_id = collection.insert_one(insert_data).inserted_id
        else:
            if dist_id is None:
                raise Exception("main为False时需提供dist_id!")
            else:
                if not isinstance(dist_id, ObjectId):
                    dist_id = ObjectId(dist_id)
        # insert detail
        with open(file_path, 'r') as f:
            l = f.readline().strip('\n')
            if not re.match(r"^\t", l):
                raise Exception("文件%s格式不正确，请选择正确的distance表格文件" % file_path)
            sample_list = l.split("\t")
            sample_list.pop(0)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                sample_name = line_data.pop(0)
                data = [("specimen_distance_id", dist_id), ("specimen_name", sample_name)]
                i = 0
                for smp in sample_list:
                    data.append((smp, float(line_data[i])))
                    i += 1
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["specimen_distance_detail"]
            collection.insert_many(data_list)
        except Exception as e:
            self.bind_object.logger.error("导入distance%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入distance%s信息成功!" % file_path)
        return dist_id
