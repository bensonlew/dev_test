# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import os
from bson.objectid import ObjectId
import types
from collections import OrderedDict


class Control(Base):
    def __init__(self, bind_object=None):
        super(Control, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_control(self, control_file, group_id):
        if not isinstance(group_id, ObjectId):
            if isinstance(group_id, types.StringTypes):
                if group_id == 'all':
                    group_id = group_id
                else:
                    group_id = ObjectId(group_id)
            else:
                raise Exception('group_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(control_file):
            raise Exception('control_file所指定的路径不存在，请检查！')
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        control_info = dict()
        with open(control_file, 'rb') as c:
            scheme_name = c.readline().strip('\n').split()[1]
            for line in c:
                line = line.strip('\n').split('\t')
                control_info[line[0]] = line[1]
        control_info = OrderedDict(sorted(control_info.items(), key=lambda t: t[0]))
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'control_names': control_info,
            'scheme_name': scheme_name,
            'group_id': group_id,
        }
        try:
            collection = self.db['sg_denovo_control']
            inserted_id = collection.insert_one(insert_data).inserted_id
        except Exception, e:
            self.bind_object.logger.error("导入对照组文件信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入对照组文件信息成功!")
            return inserted_id
