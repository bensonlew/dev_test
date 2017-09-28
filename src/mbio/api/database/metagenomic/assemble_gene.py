# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'
# last_modify:20170922
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class AssembleGene(Base):
    # 对应表格链接：http://git.majorbio.com/liu.linmeng/metagenomic/wikis/collection/assemble_gene/assemble
    def __init__(self, bind_object):
        super(AssembleGene, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_assemble_stat(self, assem_method):
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
        sequence_id = collection.insert_one(insert_data).inserted_id
        return sequence_id

    @report_check
    def add_assemble_stat_detail(self, sequence_id, stat_path):
        if not isinstance(sequence_id, ObjectId):
            if isinstance(sequence_id, types.StringTypes):
                sequence_id = ObjectId(sequence_id)
            else:
                raise Exception('sequence_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):  # 检查要上传的数据表路径是否存在
            raise Exception('stat_path所指定的路径不存在，请检查！')
        data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        with open(stat_path, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:
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
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (stat_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % stat_path)

    @report_check
    def add_gene_predict(self):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': '基因预测主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
        }
        collection = self.db['gene_predict']
        sequence_id = collection.insert_one(insert_data).inserted_id
        return sequence_id

    @report_check
    def add_gene_predict_detail(self, sequence_id, stat_path):
        if not isinstance(sequence_id, ObjectId):
            if isinstance(sequence_id, types.StringTypes):
                sequence_id = ObjectId(sequence_id)
            else:
                raise Exception('sequence_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):
            raise Exception('stat_path所指定的路径不存在，请检查！')
        data_list = list()
        with open(stat_path, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('gene_predict_id', sequence_id),
                    ('specimen_name', line[0]),
                    ('orfs', int(line[1])),
                    ('total_length', int(line[2])),
                    ('average_length', int(line[3])),
                    ('max', int(line[4])),
                    ('min', int(line[5])),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['gene_predict_detail']
            # 将detail表名称写在这里
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (stat_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % stat_path)

    @report_check
    def add_assem_bar_detail(self, sequence_id, length_path):  # 拼接预测模块长度分布图detail表
        if not isinstance(sequence_id, ObjectId):
            if isinstance(sequence_id, types.StringTypes):
                sequence_id = ObjectId(sequence_id)
            else:
                raise Exception('sequence_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.isdir(length_path):
            raise Exception('length_path所指定的路径不存在，请检查！')
        step_data = dict()
        data_list = list()
        file_list = os.listdir(length_path)
        for files in file_list:
            spe_step = files.strip().split('.')[0]
            spe = spe_step.strip().split('_step_')[0]
            step = spe_step.strip().split('_step_')[1]
            with open(os.path.join(length_path, files), 'rb') as f:
                lines = f.readlines()
                for line in lines[1:-1]:
                    line = line.strip().split('\t')
                    step_data[line[0]] = int(line[1])
            data = [
                ('assem_detail_id', sequence_id),
                ('specimen_name', spe),
                ('step', int(step)),
                ('step_data', step_data),
            ]
            data = SON(data)
            data_list.append(data)
        try:
            collection = self.db["assem_bar_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error('导入%s信息出错：%s' % (length_path, e))
        else:
            self.bind_object.logger.info('导入%s信息成功！' % length_path)
