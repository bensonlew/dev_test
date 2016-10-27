# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
# last_modify:20161027
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId
import bson.binary
from cStringIO import StringIO
import json


class DenovoNetwork(Base):
    def __init__(self, bind_object):
        super(DenovoNetwork, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_network(self, params, express_id, softpower, module, name=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_matrix_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(softpower):
            raise Exception('softpower所指定的路径:{}不存在，请检查！'.format(softpower))
        if not os.path.exists(module):
            raise Exception('module所指定的路径:{}不存在，请检查！'.format(module))
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        collection = self.db['sg_denovo_network']
        with open(softpower, 'rb') as s, open(module, 'rb') as m:
            softpower_id = StringIO(s.read())
            softpower_id = bson.binary.Binary(softpower_id.getvalue())
            module_id = StringIO(m.read())
            module_id = bson.binary.Binary(module_id.getvalue())
        params['diff_fpkm'] = str(express_id)
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'network_table_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'desc': '差异基因网络分析主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': (json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params, dict) else params),
            'status': 'end',
            'express_id': express_id,
            'softpower': softpower_id,
            'module': module_id,
        }
        network_id = collection.insert_one(insert_data).inserted_id
        return network_id

    @report_check
    def add_network_detail(self, network_id, node_path, edge_path):
        if not isinstance(network_id, ObjectId):
            if isinstance(network_id, types.StringTypes):
                network_id = ObjectId(network_id)
        if not os.path.exists(node_path):
            raise Exception('node_path所指定的路径:{}不存在，请检查！'.format(node_path))
        if not os.path.exists(edge_path):
            raise Exception('edge_path所指定的路径:{}不存在，请检查！'.format(edge_path))
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
            raise Exception('module_path所指定的路径:{}不存在，请检查！'.format(module_path))
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
