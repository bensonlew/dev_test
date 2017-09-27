# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'
# last_modify:20170923
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class MgAnnoVfdb(Base):
    def __init__(self, bind_object):
        super(MgAnnoVfdb, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_vfdb(self, geneset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(geneset_name, ObjectId):  # 检查传入的anno_vfdb_id是否符合ObjectId类型
            if isinstance(geneset_name, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_name = ObjectId(geneset_name)
            else:  # 如果是其他类型，则报错
                raise Exception('geneset_name必须为ObjectId对象或其对应的字符串！')
        core_anno_file = anno_file_path.split(",")[0]
        pre_anno_file = anno_file_path.split(",")[1]
        if not os.path.exists(core_anno_file):  # 检查要上传的数据表路径是否存在
            raise Exception('anno_file_path所指定的核心数据库注释路径不存在，请检查！')
        if not os.path.exists(pre_anno_file):
            raise Exception('anno_file_path所指定的预测数据库注释路径不存在，请检查！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': 'vfdb注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_name': geneset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_vfdb']
        # 将主表名称写在这里
        anno_vfdb_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_vfdb_id

    @report_check
    def add_anno_vfdb_vfs(self, anno_vfdb_id, vfdb_profile_dir):
        if not isinstance(anno_vfdb_id, ObjectId):  # 检查传入的anno_vfdb_id是否符合ObjectId类型
            if isinstance(anno_vfdb_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_vfdb_id = ObjectId(anno_vfdb_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_vfdb_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(vfdb_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('vfdb_profile_dir所指定的路径不存在，请检查！')
        with open(vfdb_profile_dir + "/vfdb_core_VF_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                vfs = line[0]
                species = line[-4]
                function = line[-3]
                level1 = line[-2]
                level2 = line[-1]
                insert_data = {''
                               'vfdb_id': anno_vfdb_id,
                               'vfs': vfs,
                               'species': species,
                               'function': function,
                               'level1': level1,
                               'level2': level2,
                               'data_type': "core"
                               }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_vfdb_vfs']
                anno_vfdb_vfs_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('vfs', unique=True)

    @report_check
    def add_anno_vfdb_pie(self, anno_vfdb_id, vfdb_profile_dir):
        if not isinstance(anno_vfdb_id, ObjectId):  # 检查传入的anno_vfdb_id是否符合ObjectId类型
            if isinstance(anno_vfdb_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_vfdb_id = ObjectId(anno_vfdb_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_vfdb_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(vfdb_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('vfdb_profile_dir所指定的路径不存在，请检查！')
        with open(vfdb_profile_dir + "/vfdb_level_pie.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                level1 = line[0]
                level2 = line[1]
                abu = line[2]
                percent = line[3]
                insert_data = {
                    'vfdb_id': anno_vfdb_id,
                    'level1': level1,
                    'level2': level2,
                    'abu': abu,
                    'percent': percent
                }
                """
                for sam in sams:
                    insert_data[sam] = sam
              """
                collection = self.db['anno_vfdb_pie']
                anno_vfdb_pie_id = collection.insert_one(insert_data).inserted_id
