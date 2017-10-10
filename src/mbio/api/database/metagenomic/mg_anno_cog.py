# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'
# last_modify:20170922
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class MgAnnoCog(
    Base):  # 导表函数，对应表格链接：http://git.majorbio.com/liu.linmeng/metagenomic/wikis/collection/assemble_gene/assemble
    def __init__(self, bind_object):
        super(MgAnnoCog, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_cog(self, geneset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(geneset_name, ObjectId):  # 检查传入的anno_cog_id是否符合ObjectId类型
            if isinstance(geneset_name, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_name = ObjectId(geneset_name)
            else:  # 如果是其他类型，则报错
                raise Exception('geneset_name必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(anno_file_path):  # 检查要上传的数据表路径是否存在
            raise Exception('anno_file_path所指定的路径不存在，请检查！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': 'COG注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_name': geneset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_cog']
        # 将主表名称写在这里
        anno_cog_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_cog_id

    @report_check
    def add_anno_cog_nog(self, anno_cog_id, cog_profile_dir):
        if not isinstance(anno_cog_id, ObjectId):  # 检查传入的anno_cog_id是否符合ObjectId类型
            if isinstance(anno_cog_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_cog_id = ObjectId(anno_cog_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_cog_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cog_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('cog_profile_dir所指定的路径不存在，请检查！')
        with open(cog_profile_dir + "/cog_nog_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                nog = line[0]
                des = line[-1]
                insert_data = {
                    'cog_id': anno_cog_id,
                    'nog': nog,
                    'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_cog_nog']
                anno_cog_nog_id = collection.insert_one(insert_data).inserted_id
            collection.ensure_index('nog', unique=False)

    @report_check
    def add_anno_cog_function(self, anno_cog_id, cog_profile_dir):
        if not isinstance(anno_cog_id, ObjectId):  # 检查传入的anno_cog_id是否符合ObjectId类型
            if isinstance(anno_cog_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_cog_id = ObjectId(anno_cog_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_cog_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cog_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('cog_profile_dir所指定的路径不存在，请检查！')
        with open(cog_profile_dir + "/cog_function_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                function = line[0]
                des = line[-1]
                insert_data = {
                    'cog_id': anno_cog_id,
                    'function': function,
                    'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_cog_function']
                anno_cog_function_id = collection.insert_one(insert_data).inserted_id

    @report_check
    def add_anno_cog_category(self, anno_cog_id, cog_profile_dir):
        if not isinstance(anno_cog_id, ObjectId):  # 检查传入的anno_cog_id是否符合ObjectId类型
            if isinstance(anno_cog_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_cog_id = ObjectId(anno_cog_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_cog_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cog_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('cog_profile_dir所指定的路径不存在，请检查！')
        with open(cog_profile_dir + "/cog_category_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            sams = head.strip().split("\t")[1:len(head) - 1]
            for line in f:
                line = line.strip().split('\t')
                category = line[0]
                des = line[-1]
                insert_data = {
                    'cog_id': anno_cog_id,
                    'category': category,
                    'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_cog_category']
                anno_cog_category_id = collection.insert_one(insert_data).inserted_id
