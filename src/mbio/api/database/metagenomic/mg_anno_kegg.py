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


class MgAnnoKegg(Base):
    def __init__(self, bind_object):
        super(MgAnnoKegg, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_kegg(self, geneset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(geneset_name, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
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
            'desc': 'kegg注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_name': geneset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_kegg']
        # 将主表名称写在这里
        anno_kegg_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_kegg_id

    @report_check
    def add_anno_kegg_gene(self, anno_kegg_id, kegg_profile_dir):
        if not isinstance(anno_kegg_id, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
            if isinstance(anno_kegg_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_kegg_id = ObjectId(anno_kegg_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('kegg_profile_dir所指定的路径不存在，请检查！')
        with open(kegg_profile_dir + "/kegg_gene_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            heads = head.strip().split("\t")
            sams = heads[1:len(heads) - 1]
            for line in f:
                line = line.strip().split('\t')
                gene = line[0]
                #KO = line[1]
                insert_data = {
                    'kegg_id': anno_kegg_id,
                    'gene': gene,
                    #'orthology':KO
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_kegg_gene']
                anno_kegg_gene_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('gene', unique=False)

    @report_check
    def add_anno_kegg_orthology(self, anno_kegg_id, kegg_profile_dir):
        if not isinstance(anno_kegg_id, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
            if isinstance(anno_kegg_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_kegg_id = ObjectId(anno_kegg_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('kegg_profile_dir所指定的路径不存在，请检查！')
        with open(kegg_profile_dir + "/kegg_KO_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            heads = head.strip().split("\t")
            sams = heads[1:len(heads) - 1]
            for line in f:
                line = line.strip().split('\t')
                KO = line[0]
                #des = line[-1]
                insert_data = {
                    'kegg_id': anno_kegg_id,
                    'orthology':KO,
                    #'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_kegg_orthology']
                anno_kegg_orthology_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('orthology', unique=False)

    @report_check
    def add_anno_kegg_module(self, anno_kegg_id, kegg_profile_dir):
        if not isinstance(anno_kegg_id, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
            if isinstance(anno_kegg_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_kegg_id = ObjectId(anno_kegg_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('kegg_profile_dir所指定的路径不存在，请检查！')
        with open(kegg_profile_dir + "/kegg_module_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            heads = head.strip().split("\t")
            sams = heads[1:len(heads) - 2]
            for line in f:
                line = line.strip().split('\t')
                module = line[0]
                des = line[-1]
                insert_data = {
                    'kegg_id': anno_kegg_id,
                    'module': module,
                    'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_kegg_module']
                anno_kegg_module_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('module', unique=False)

    @report_check
    def add_anno_kegg_enzyme(self, anno_kegg_id, kegg_profile_dir):
        if not isinstance(anno_kegg_id, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
            if isinstance(anno_kegg_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_kegg_id = ObjectId(anno_kegg_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('kegg_profile_dir所指定的路径不存在，请检查！')
        with open(kegg_profile_dir + "/kegg_enzyme_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            heads = head.strip().split("\t")
            sams = heads[1:len(heads) - 2]
            for line in f:
                line = line.strip().split('\t')
                enzyme = line[0]
                des = line[-1]
                insert_data = {
                    'kegg_id': anno_kegg_id,
                    'enzyme': enzyme,
                    'description': des
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_kegg_enzyme']
                anno_kegg_enzyme_id = collection.insert_one(insert_data).inserted_id
        collection.ensure_index('enzyme', unique=False)

    @report_check
    def add_anno_kegg_pathway(self, anno_kegg_id, kegg_profile_dir):
        if not isinstance(anno_kegg_id, ObjectId):  # 检查传入的anno_kegg_id是否符合ObjectId类型
            if isinstance(anno_kegg_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_kegg_id = ObjectId(anno_kegg_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('kegg_profile_dir所指定的路径不存在，请检查！')
        with open(kegg_profile_dir + "/kegg_pathway_profile.xls", 'rb') as f:
            head = f.next()  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
            heads = head.strip().split("\t")
            sams = heads[1:len(heads) - 3]
            for line in f:
                line = line.strip().split('\t')
                pathway = line[0]
                des = line[-2]
                map = line[-1]
                insert_data = {
                    'kegg_id': anno_kegg_id,
                    'pathway':pathway,
                    'description': des,
                    'pathwaymap': map
                }
                for i in range(0,len(sams)):
                    insert_data[sams[i]] = line[i+1]
                collection = self.db['anno_kegg_pathway']
                anno_kegg_pathway_id = collection.insert_one(insert_data).inserted_id
        with open(kegg_profile_dir + "/kegg_pathway_img_id.xls", 'rb') as f2: 
            head = f2.next()
            for line in f2:
                line = line.strip().split('\t')
                ko = line[0]
                img_id = line[1]
                if isinstance(img_id, types.StringTypes):
                     img_id = ObjectId(img_id)
                collection.update({'pathway': ko}, {'$set': {'pathwayimg': img_id}})
        collection.ensure_index('pathway', unique=False)



