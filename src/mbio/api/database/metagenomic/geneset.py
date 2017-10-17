# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
# last_modify:20170921
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class Geneset(Base):
    def __init__(self, bind_object):
        super(Geneset, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_geneset(self, file_path, type=1, params=None):
        if not type in [1, 2]:  # 1对应origin表，包含6个丰度文件路径，无gene_list文件，2对应筛选后的基因表格，则无丰度文件路径
            raise OptionError("type必须为1,或2")
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if not os.path.exists(file_path + '/uniGeneset/geneCatalog_stat.xls'):
            raise Exception('geneCatalog_stat.xls文件不存在，请检查！')
        with open(file_path + '/uniGeneset/geneCatalog_stat.xls', 'rb') as f:
            lines = f.readlines()
            line = lines[1].strip().split('\t')
            catalog_genes = int(line[0])
            catalog_total_length = int(line[1])
            catalog_average_length = round(float(line[2]), 2)
        if not os.path.exists(file_path + '/gene_profile/reads_number.xls'):
            raise Exception('reads_number.xls文件不存在，请检查！')
        if not os.path.exists(file_path + '/gene_profile/reads_number_relative.xls'):
            raise Exception('reads_number_relative.xls文件不存在，请检查！')
        os.system(
            'tar czPf ' + file_path + '/gene_profile/reads_profile.tar.gz ' + file_path + '/gene_profile/reads_number.xls ' + file_path + '/gene_profile/reads_number_relative.xls')
        with open(file_path + '/gene_profile/reads_number.xls', 'rb') as file:
            line = file.readline().strip().split('\t')
            specimen = ",".join(line[1:-1])
        if type == 1:
            insert_data = {
                'project_sn': project_sn,
                'task_id': task_id,
                'desc': '',
                'created_ts': created_ts,
                'name': 'null',
                'params': params,
                'status': 'end',
                'catalog_genes': catalog_genes,
                'catalog_total_length': catalog_total_length,
                'catalog_average_length': catalog_average_length,
                'type': type,
                'specimen': specimen,
                'reads_num': file_path + '/gene_profile/reads_number.xls',
                'reads_num_relative': file_path + '/gene_profile/reads_number_relative.xls',
                'rpkm': file_path + '/gene_profile/RPKM.xls',
                'tpm': file_path + '/gene_profile/TPM.xls',
                'reads_genelen_ratio': file_path + '/gene_profile/reads_length_ratio.xls',
                'reads_genelen_ratio_relative': file_path + '/gene_profile/reads_number_relative.xls',
                'download_file': file_path + '/gene_profile/reads_profile.tar.gz',
                'gene_list_length': file_path + '/gene_profile/gene.uniGeneset.fa.length.txt'
            }
        else:
            insert_data = {
                'project_sn': project_sn,
                'task_id': task_id,
                'desc': '',
                'created_ts': created_ts,
                'name': 'null',
                'params': params,
                'status': 'end',
                'catalog_genes': catalog_genes,
                'catalog_total_length': catalog_total_length,
                'catalog_average_length': catalog_average_length,
                'type': type,
                'specimen': specimen,
                'download_file': file_path + '/gene_profile/reads_profile.tar.gz',
                'gene_list_length': file_path + '/gene_profile/gene.uniGeneset.fa.length.txt'
            }
        collection = self.db['geneset']
        # 将主表名称写在这里
        geneset_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return geneset_id

    @report_check
    def add_geneset_bar(self, geneset_id, length_path):  # 序列长度分布图
        if not isinstance(geneset_id, ObjectId):  # 检查传入的geneset_id是否符合ObjectId类型
            if isinstance(geneset_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_id = ObjectId(geneset_id)
            else:  # 如果是其他类型，则报错
                raise Exception('geneset_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.isdir(length_path):  # 检查要上传的数据表路径是否存在
            raise Exception('length_path所指定的文件及不存在，请检查！')
        for filename in os.listdir(length_path):
            step_data = dict()
            spe_step = filename.strip().split('.')[0]
            spe = spe_step.strip().split('_step_')[0]
            step = spe_step.strip().split('_step_')[1]
            with open(os.path.join(length_path, filename), 'rb') as f:
                lines = f.readlines()
                for line in lines[1:-1]:
                    line = line.strip().split('\t')
                    step_data[line[0]] = int(line[1])
            data = {
                'geneset_id': geneset_id,
                'specimen_name': spe,
                'step': int(step),
                'step_data': step_data,
            }
            try:
                collection = self.db["geneset_bar"]
                collection.insert_one(data)
            except Exception, e:
                self.bind_object.logger.error('导入%s信息出错：%s' % (filename, e))
            else:
                self.bind_object.logger.info('导入%s信息成功！' % filename)

    @report_check
    def add_geneset_readsn(self, geneset_id, readsnum_path):  # 丰度前100的基因的reads number
        if not isinstance(geneset_id, ObjectId):  # 检查传入的geneset_id是否符合ObjectId类型
            if isinstance(geneset_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_id = ObjectId(geneset_id)
            else:  # 如果是其他类型，则报错
                raise Exception('geneset_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(readsnum_path):
            raise Exception('readsnum_path所指定的路径不存在，请检查！')
        data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        with open(readsnum_path, 'rb') as f:
            lines = f.readlines()
            line0 = lines[0].strip().split('\t')
            sample = line0[1:]
            for line in lines[1:]:
                data = [('geneset_id', geneset_id)]
                line = line.strip().split('\t')
                data.append(('gene_id', line[0]))
                i = 1
                for eachsample in sample:
                    data.append((eachsample, int(line[i])))
                    i += 1
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['geneset_readsn']
            # 将detail表名称写在这里
            collection.insert_many(data_list)  # 用insert_many批量导入数据库，insert_one一次只能导入一条记录
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (readsnum_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % readsnum_path)

    @report_check
    def add_geneset_readsr(self, geneset_id, readsnum_relative):  # 丰度前100的基因的reads number relative
        if not isinstance(geneset_id, ObjectId):  # 检查传入的geneset_id是否符合ObjectId类型
            if isinstance(geneset_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_id = ObjectId(geneset_id)
            else:  # 如果是其他类型，则报错
                raise Exception('geneset_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(readsnum_relative):
            raise Exception('readsnum_relative所指定的路径不存在，请检查！')
        data_list = list()  # 存入表格中的信息，然后用insert_many批量导入
        with open(readsnum_relative, 'rb') as f:
            lines = f.readlines()
            line0 = lines[0].strip().split('\t')
            sample = line0[1:]
            for line in lines[1:]:
                data = [('geneset_id', geneset_id)]
                line = line.strip().split('\t')
                data.append(('gene_id', line[0]))
                i = 1
                for eachsample in sample:
                    data.append((eachsample, float(line[i])))
                    i += 1
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['geneset_readsr']
            # 将detail表名称写在这里
            collection.insert_many(data_list)  # 用insert_many批量导入数据库，insert_one一次只能导入一条记录
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (readsnum_relative, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % readsnum_relative)
