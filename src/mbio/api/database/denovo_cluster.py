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
import json


class DenovoCluster(Base):
    def __init__(self, bind_object):
        super(DenovoCluster, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_cluster(self, params, express_id, sample_tree=None, gene_tree=None, name=None, samples=None, genes=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if gene_tree:
            with open(gene_tree, 'rb') as g:
                gene_tree = g.readlines()[0].strip('\n')
        if sample_tree:
            with open(sample_tree, 'rb') as s:
                sample_tree = s.readlines()[0].strip('\n')
        params['diff_fpkm'] = str(express_id)
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'cluster_table_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'desc': '差异基因聚类分析主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params': (json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params, dict) else params),
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
            raise Exception('sub_path所指定的路径:{}不存在，请检查！'.format(sub_path))
        data_list = []
        with open(sub_path, 'rb') as f:
            head = f.readline().strip().split('\t')
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('sub_cluster', int(sub)),
                    ('cluster_id', cluster_id),
                    ('gene_id', line[0])
                ]
                for i in range(len(head)):
                    data.append((head[i], round(float(line[i + 1]), 4)))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_cluster_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入子聚类统计表：%s信息出错:%s" % (sub_path, e))
        else:
            self.bind_object.logger.info("导入子聚类统计表:%s信息成功!" % sub_path)
