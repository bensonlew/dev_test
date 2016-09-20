# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
# last_modify:20160919
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class DenovoExpress(Base):
    def __init__(self, bind_object):
        super(DenovoExpress, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_express(self, samples=None, params=None, name=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'express_matrix',
            'desc': '表达量计算主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': params,
            'specimen': samples,
            'status': 'end',
        }
        collection = self.db['sg_denovo_express']
        express_id = collection.insert_one(insert_data).inserted_id
        return express_id

    @report_check
    def add_express_detail(self, express_id, count_path, fpkm_path, path_type):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(count_path):
            raise Exception('count_path所指定的路径不存在，请检查！')
        if not os.path.exists(fpkm_path):
            raise Exception('fpkm_path所指定的路径不存在，请检查！')
        data_list = list()
        count_dict = {}
        sample_count = {}
        with open(count_path, 'rb') as c, open(fpkm_path, 'rb') as f:
            samples = c.readline().strip().split('\t')
            for sam in samples:
                sample_count[sam] = 0
            for line in c:
                line = line.strip().split('\t')
                count_dict[line[0]] = line[1:]
                count = line[1:]
                for i in range(len(count)):
                    if float(count[i]) > 0:
                        sample_count[samples[i]] += 1
            f.readline()
            for l in f:
                l = l.strip().split('\t')
                gene_id = l[0]
                fpkm = l[1:]
                data = [
                    ('gene_id', gene_id),
                    ('type', path_type),
                    ('express_id', express_id),
                ]
                for i in range(len(samples)):
                    data += [
                        ('{}_count'.format(samples[i]), count_dict[gene_id][i]), ('{}_fpkm'.format(samples[i]), fpkm[i]),
                        ('{}_sum'.format(samples[i]), sample_count[samples[i]]),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_express_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入表达量矩阵信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入表达量矩阵信息成功!")

    @report_check
    def add_express_specimen_detail(self, express_id, rsem_result, rsem_type, sample=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(rsem_result):
            raise Exception('rsem_result所指定的路径不存在，请检查！')
        sample_name = os.path.basename(rsem_result).split('.')[0]
        data_list = []
        with open(rsem_result, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('express_id', express_id),
                    ('specimen_name', sample if sample else sample_name),
                    ('type', rsem_type),
                    ('length', line[2]),
                    ('effective_length', line[3]),
                    ('expected_count', line[4]),
                    ('TPM', line[5]),
                    ('FPKM', line[6]),
                ]
                if rsem_type == 'gene':
                    data += [
                        ('gene_id', line[0]),
                        ('transcript_id', line[1]),
                    ]
                else:
                    data += [
                        ('gene_id', line[1]),
                        ('transcript_id', line[0]),
                        ('IsoPct', line[7]),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_express_specimen_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入单样本表达量矩阵：%s信息出错:%s" % (rsem_result, e))
        else:
            self.bind_object.logger.info("导入单样本表达量矩阵: %s信息成功!" % rsem_result)

    @report_check
    def add_express_diff(self, params, samples, compare_column, name=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'gene_express_diff_stat',
            'desc': '表达量差异检测主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': params,
            'specimen': samples,
            'status': 'end',
            'compare_column': compare_column,
        }
        collection = self.db['sg_denovo_express_diff']
        express_diff_id = collection.insert_one(insert_data).inserted_id
        return express_diff_id

    @report_check
    def add_express_diff_detail(self, express_diff_id, group, diff_stat_path):
        if not isinstance(express_diff_id, ObjectId):
            if isinstance(express_diff_id, types.StringTypes):
                express_diff_id = ObjectId(express_diff_id)
            else:
                raise Exception('express_diff_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(diff_stat_path):
            raise Exception('diff_stat_path所指定的路径不存在，请检查！')
        data_list = []
        with open(diff_stat_path, 'rb') as f:
            head = f.readline().strip().split('\t')
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('name', group[0]),
                    ('compare_name', group[1]),
                    ('express_diff_id', express_diff_id),
                ]
                for i in range(len(head)):
                    data.append((head[i], line[i]))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_express_diff_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入基因表达差异统计表：%s信息出错:%s" % (diff_stat_path, e))
        else:
            self.bind_object.logger.info("导入基因表达差异统计表：%s信息成功!" % diff_stat_path)

    @report_check
    def add_cluster(self, params, samples, express_id, sample_tree, gene_tree, genes, name=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'cluster_table',
            'desc': '差异基因聚类分析主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': params,
            'specimen': samples,
            'status': 'end',
            'express_id': express_id,
            'sample_tree': sample_tree,
            'gene_tree': gene_tree,
            'genes': genes,
        }
        collection = self.db['sg_denovo_cluster']
        cluster_id = collection.insert_one(insert_data).inserted_id
        return cluster_id

    @report_check
    def add_cluster_detail(self, cluster_id, sub, sub_path):
        if not isinstance(cluster_id, ObjectId):
            if isinstance(cluster_id, types.StringTypes):
                cluster_id = ObjectId(cluster_id)
            else:
                raise Exception('cluster_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(sub_path):
            raise Exception('sub_path所指定的路径不存在，请检查！')
        data_list = []
        with open(sub_path, 'rb') as f:
            head = f.readline().strip().split('\t')
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('sub_cluster', sub),
                    ('cluster_id', cluster_id),
                    ('gene_id', line[0])
                ]
                for i in range(len(head)):
                    data.append((head[i], line[i + 1]))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_cluster_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入子聚类统计表：%s信息出错:%s" % (sub_path, e))
        else:
            self.bind_object.logger.info("导入子聚类统计表:%s信息成功!" % sub_path)

    @report_check
    def add_network(self, params, express_id, softpower_id, module_id, name=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_matrix_id必须为ObjectId对象或其对应的字符串！')
        if not isinstance(softpower_id, ObjectId):
            if isinstance(softpower_id, types.StringTypes):
                softpower_id = ObjectId(softpower_id)
            else:
                raise Exception('softpower_id必须为ObjectId对象或其对应的字符串！')
        if not isinstance(module_id, ObjectId):
            if isinstance(module_id, types.StringTypes):
                module_id = ObjectId(module_id)
            else:
                raise Exception('module_id必须为ObjectId对象或其对应的字符串！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'network_table',
            'desc': '差异基因网络分析主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': params,
            'status': 'end',
            'express_id': express_id,
            'softpower': softpower_id,
            'module': module_id,
        }
        collection = self.db['sg_denovo_network']
        network_id = collection.insert_one(insert_data).inserted_id
        return network_id

    @report_check
    def add_network_detail(self, network_id, node_path, edge_path):
        if not isinstance(network_id, ObjectId):
            if isinstance(network_id, types.StringTypes):
                network_id = ObjectId(network_id)
        if not os.path.exists(node_path):
            raise Exception('node_path所指定的路径不存在，请检查！')
        if not os.path.exists(edge_path):
            raise Exception('edge_path所指定的路径不存在，请检查！')
        data_list = []
        gene_color = {}
        with open(node_path, 'rb') as n, open(edge_path, 'rb') as f:
            n.readline()
            for line in n:
                line = line.strip().split('\t')
                gene_color[line[0]] = line[2]
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('network_id', network_id),
                    ('gene_id1', {'name': line[0], 'color': gene_color[line[0]]}),
                    ('gene_id2', {'name': line[1], 'color': gene_color[line[1]]}),
                    ('weight', line[2]),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_network_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入网络表达统计表：%s，%s信息出错:%s" % (node_path, edge_path, e))
        else:
            self.bind_object.logger.info("导入网络表达统计表:%s， %s信息成功!" % (node_path, edge_path))

    @report_check
    def add_network_module(self, network_id, module_path, module_color):
        if not isinstance(network_id, ObjectId):
            if isinstance(network_id, types.StringTypes):
                network_id = ObjectId(network_id)
        if not os.path.exists(module_path):
            raise Exception('module_path所指定的路径不存在，请检查！')
        data_list = []
        with open(module_path, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('network_id', network_id),
                    ('gene_id1', line[0]),
                    ('gene_id2', line[1]),
                    ('weight', line[2]),
                    ('module_color', module_color),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_network_module"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入网络表达统计表：%s信息出错:%s" % (module_path, e))
        else:
            self.bind_object.logger.info("导入网络表达统计表:%s信息成功!" % module_path)
