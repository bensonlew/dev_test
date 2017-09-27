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


class MgAnnoArdb(Base):
    def __init__(self, bind_object):
        super(MgAnnoArdb, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_ardb(self, argset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(argset_name, ObjectId):  # 检查传入的anno_ardb_id是否符合ObjectId类型
            if isinstance(argset_name, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                argset_name = ObjectId(argset_name)
            else:  # 如果是其他类型，则报错
                raise Exception('argset_name必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(anno_file_path):  # 检查要上传的数据表路径是否存在
            raise Exception('anno_file_path所指定的路径不存在，请检查！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': 'ardb注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'argset_name': argset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_ardb']
        # 将主表名称写在这里
        anno_ardb_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_ardb_id

    @report_check
    def add_anno_ardb_arg(self, anno_ardb_id, ardb_profile_dir):
        if not isinstance(anno_ardb_id, ObjectId):  # 检查传入的anno_ardb_id是否符合ObjectId类型
            if isinstance(anno_ardb_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_ardb_id = ObjectId(anno_ardb_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_ardb_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(ardb_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('ardb_profile_dir所指定的路径不存在，请检查！')
        with open(ardb_profile_dir + "/ardb.ARG.profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                arg = line[0]
                # KO = line[1]
                insert_data = {
                    'ardb_id': anno_ardb_id,
                    'arg': arg,
                    # 'orthology':KO
                }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_ardb_arg']
                anno_ardb_arg_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('arg', unique=True)

    @report_check
    def add_anno_ardb_type(self, anno_ardb_id, ardb_profile_dir):
        if not isinstance(anno_ardb_id, ObjectId):  # 检查传入的anno_ardb_id是否符合ObjectId类型
            if isinstance(anno_ardb_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_ardb_id = ObjectId(anno_ardb_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_ardb_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(ardb_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('ardb_profile_dir所指定的路径不存在，请检查！')
        with open(ardb_profile_dir + "/ardb.type.profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                type = line[0]
                # antibiotic_type = line[-1]
                insert_data = {
                    'ardb_id': anno_ardb_id,
                    'type': type,
                    # 'antibiotic_type': antibiotic_type
                }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_ardb_type']
                anno_ardb_type_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('type', unique=True)

    @report_check
    def add_anno_ardb_class(self, anno_ardb_id, ardb_profile_dir):
        if not isinstance(anno_ardb_id, ObjectId):  # 检查传入的anno_ardb_id是否符合ObjectId类型
            if isinstance(anno_ardb_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_ardb_id = ObjectId(anno_ardb_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_ardb_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(ardb_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('ardb_profile_dir所指定的路径不存在，请检查！')
        with open(ardb_profile_dir + "/ardb.class.profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                classname = line[0]
                # class_des = line[-1]
                insert_data = {
                    'ardb_id': anno_ardb_id,
                    'class': classname,
                    # 'class_des': class_des
                }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_ardb_class']
                anno_ardb_class_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('class', unique=True)
