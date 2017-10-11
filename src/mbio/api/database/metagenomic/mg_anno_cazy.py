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


class MgAnnoCazy(Base):
    def __init__(self, bind_object):
        super(MgAnnoCazy, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_cazy(self, familyset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(familyset_name, ObjectId):  # 检查传入的anno_cazy_id是否符合ObjectId类型
            if isinstance(familyset_name, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                familyset_name = ObjectId(familyset_name)
            else:  # 如果是其他类型，则报错
                raise Exception('familyset_name必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(anno_file_path):  # 检查要上传的数据表路径是否存在
            raise Exception('anno_file_path所指定的路径不存在，请检查！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': 'cazy注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'familyset_name': familyset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_cazy']
        # 将主表名称写在这里
        anno_cazy_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_cazy_id
    
    @report_check
    def add_anno_cazy_family(self, anno_cazy_id, cazy_profile_dir):
        if not isinstance(anno_cazy_id, ObjectId):  # 检查传入的anno_cazy_id是否符合ObjectId类型
            if isinstance(anno_cazy_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_cazy_id = ObjectId(anno_cazy_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_cazy_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cazy_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('cazy_profile_dir所指定的路径不存在，请检查！')
        with open(cazy_profile_dir + "/cazy_family_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                family = line[0]
                # description = line[-1]
                insert_data = {
                    'cazy_id': anno_cazy_id,
                    'family': family,
                    #'description': description
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i]
                collection = self.db['anno_cazy_family']
                anno_cazy_family_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('family', unique=False)

    @report_check
    def add_anno_cazy_class(self, anno_cazy_id, cazy_profile_dir):
        if not isinstance(anno_cazy_id, ObjectId):  # 检查传入的anno_cazy_id是否符合ObjectId类型
            if isinstance(anno_cazy_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_cazy_id = ObjectId(anno_cazy_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_cazy_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cazy_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('cazy_profile_dir所指定的路径不存在，请检查！')
        with open(cazy_profile_dir + "/cazy_class_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                classes = line[0]
                # description = line[-1]
                insert_data = {
                    'cazy_id': anno_cazy_id,
                    'class': classes,
                    #'description': description
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i]
                collection = self.db['anno_cazy_class']
                anno_cazy_class_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('class', unique=False)
