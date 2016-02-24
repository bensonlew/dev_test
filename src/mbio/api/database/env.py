# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.api.database.base import Base, report_check
import os
import datetime
import types


class Env(Base):
    def __init__(self, bind_object=None):
        super(Env, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_env_table(self, file_path, name_to_id, task_id=None, name=None):
        if isinstance(name_to_id, types.DictType):
            pass
        else:
            raise Exception('错误的name_to_id类型，或者没有提供')
        if not task_id:
            task_id = self.bind_object.sheet.id
        names, details = self._get_table_info(file_path, name_to_id)
        main_insert_data = {
            'project_sn': self.bind_object.sheet.project_sn,
            'task_id': task_id,
            'name': name if name else 'origin_env_table',
            'env_names': ','.join(names),
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        main_collection = self.db['sg_env']
        collections = self.db['sg_env_detail']
        main_env_id = main_collection.insert_one(main_insert_data).inserted_id
        for sp_id, sp_dict in details.items():  # "details" is a dict, and "details"'s values is dict too
            sp_dict['env_id'] = main_env_id
            sp_dict['specimen_id'] = sp_id
            collections.insert_one(sp_dict).inserted_id
        return main_env_id

    def _get_table_info(self, file_path, name_to_id):
        with open(file_path, 'rb') as f:
            lines = f.readlines()
            env_names = lines[0].rstrip().split('\t')[1:]
            info_dic = {}
            for line in lines[1:]:
                values = line.rstrip().split('\t')
                if values[0] not in name_to_id:
                    raise Exception('样本：%s不在原始otu表的样本中' % values[0])
                info_dic[name_to_id[values[0]]] = dict(zip(env_names, values[1:]))
            return env_names, info_dic
