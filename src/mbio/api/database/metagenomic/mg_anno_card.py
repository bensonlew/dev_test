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


class MgAnnoCard(Base):
    def __init__(self, bind_object):
        super(MgAnnoCard, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'
        
        
    @report_check
    def add_anno_card(self, argset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(argset_name, ObjectId):  # 检查传入的anno_card_id是否符合ObjectId类型
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
            'desc': 'card注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'argset_name': argset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_card']
        # 将主表名称写在这里
        anno_card_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_card_id

    @report_check
    def add_anno_card_aro(self, anno_card_id, card_profile_dir):
        if not isinstance(anno_card_id, ObjectId):  # 检查传入的anno_card_id是否符合ObjectId类型
            if isinstance(anno_card_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_card_id = ObjectId(anno_card_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_card_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(card_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('card_profile_dir所指定的路径不存在，请检查！')
        with open(card_profile_dir + "/card.ARO.profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                ARO = line[0]
                aro_name = line[1]
                description = line[2]
                insert_data = {
                    'card_id': anno_card_id,
                    'aro': ARO ,
                    'aro_name': aro_name,
                    'description': description
                }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_card_aro']
                anno_card_arg_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('aro', unique=True)

    @report_check
    def add_anno_card_class(self, anno_card_id, card_profile_dir):
        if not isinstance(anno_card_id, ObjectId):  # 检查传入的anno_card_id是否符合ObjectId类型
            if isinstance(anno_card_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_card_id = ObjectId(anno_card_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_card_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(card_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('card_profile_dir所指定的路径不存在，请检查！')
        with open(card_profile_dir + "/card.class.profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                classes = line[0]
                # description = line[-1]
                insert_data = {
                    'card_id': anno_card_id,
                    'class': classes,
                    # 'description': description
                }
                for sam in sams:
                    insert_data[sam] = sam
                collection = self.db['anno_card_class']
                anno_card_class_id = collection.insert_one(insert_data).inserted_id
