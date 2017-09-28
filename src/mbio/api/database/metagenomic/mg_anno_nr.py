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


class AnnoNr(
    Base):  # 导表函数，对应表格链接：http://git.majorbio.com/liu.linmeng/metagenomic/wikis/collection/assemble_gene/assemble
    def __init__(self, bind_object):
        super(AnnoNr, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_nr(self, geneset_name, specimen, anno_file_path):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(geneset_name, ObjectId):  # 检查传入的anno_nr_id是否符合ObjectId类型
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
            'desc': '物种注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_name': geneset_name,
            'specimen': specimen,
            'anno_file': anno_file_path
        }
        collection = self.db['anno_nr']
        # 将主表名称写在这里
        anno_nr_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_nr_id

    @report_check
    def add_anno_nr_detail(self, anno_nr_id, nr_profile_dir):
        if not isinstance(anno_nr_id, ObjectId):  # 检查传入的anno_nr_id是否符合ObjectId类型
            if isinstance(anno_nr_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_nr_id = ObjectId(anno_nr_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_nr_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(nr_profile_dir):  # 检查要上传的数据表路径是否存在
            raise Exception('nr_profile_dir所指定的路径不存在，请检查！')
        # data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        f_d = os.path.join(nr_profile_dir,"tax_d.xls")
        f_k = os.path.join(nr_profile_dir,"tax_k.xls")
        f_p = os.path.join(nr_profile_dir,"tax_p.xls")
        f_c = os.path.join(nr_profile_dir,"tax_c.xls")
        f_o = os.path.join(nr_profile_dir,"tax_o.xls")
        f_g = os.path.join(nr_profile_dir,"tax_g.xls")
        f_s = os.path.join(nr_profile_dir,"tax_s.xls")
        file_list = [f_d,f_k,f_p,f_c,f_o,f_g,f_s]
        level_list = ['d','k','p','c','o','g','s']
        i = 0
        sam_num = 0
        for each in file_list:
            i += 1
            print i
            print level_list[i-1]
            if os.path.exists(each):
                with open(each, 'rb') as f:
                    head = f.next() # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
                    sams = head.strip().split("\t")[1:len(head)]
                    print sams
                    for line in f:
                        line = line.strip().split('\t')
                        tax = line[0].split(";")
                        insert_data = {
                            'level_id': level_list[i-1],
                            'nr_id': anno_nr_id,
                        }
                        for sam in sams:
                            insert_data[sam] = sam
                        if len(tax) >= 1 :
                            insert_data['d__'] = tax[0]
                        elif len(tax) >= 2:
                            insert_data['k__'] = tax[1]
                        elif len(tax) >= 3:
                            insert_data['p__'] = tax[2]
                        elif len(tax) >= 4:
                            insert_data['c__'] = tax[3]
                        elif len(tax) >= 5:
                            insert_data['f__'] = tax[4]
                        elif len(tax) >= 6:
                            insert_data['g__'] = tax[5]
                        elif len(tax) >= 7:
                            insert_data['s__'] = tax[6]
                        else:
                            raise Exception('tax文件不正确！')
                        # data = SON(data)
                        collection = self.db['anno_nr_detail']
                        anno_nr_detail_id = collection.insert_one(insert_data).inserted_id
            else:
                raise Exception('tax文件不存在！')
