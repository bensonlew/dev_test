# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'
# last_modify:20170921
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class Assemble(Base):  # 导表函数，对应表格链接：http://git.majorbio.com/liu.linmeng/metagenomic/wikis/collection/assemble_gene/assemble
    def __init__(self, bind_object):
        super(Assemble, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_assemble_stat(self, assem_method):
    # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': '组装拼接主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'assem_method': assem_method,
        }
        collection = self.db['assemble_stat']
        # 将主表名称写在这里
        sequence_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return sequence_id

    @report_check
    def add_assemble_stat_detail(self, sequence_id, stat_path):  # 拼接质量detail表, 需要主表_id以及要上传数据库的文件的路径
        if not isinstance(sequence_id, ObjectId):  # 检查传入的sequence_id是否符合ObjectId类型
            if isinstance(sequence_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                sequence_id = ObjectId(sequence_id)
            else:                                  # 如果是其他类型，则报错
                raise Exception('sequence_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):  # 检查要上传的数据表路径是否存在
            raise Exception('stat_path所指定的路径不存在，请检查！')
        data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        with open(stat_path, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
                line = line.strip().split('\t')
                data = [
                    ('assem_id', sequence_id),
                    ('specimen_name', line[0]),
                    ('contigs', int(line[1])),
                    ('contigs_bases', int(line[2])),
                    ('n50', int(line[3])),
                    ('n90', int(line[4])),
                    ('max', int(line[5])),
                    ('min', int(line[6])),
                ]
                if line[6] not in ['Idba_Mix', 'Megahit_Mix', 'Newbler_Mix']:
                    data.append(('method', 'simple'))
                else:
                    data.append(('method', 'multiple'))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['assemble_stat_detail']
            # 将detail表名称写在这里
            collection.insert_many(data_list)  # 用insert_many批量导入数据库，insert_one一次只能导入一条记录
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (stat_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % stat_path)

    @report_check
    def add_assem_bar_detail(self):  # 长度分布图detail表
        pass