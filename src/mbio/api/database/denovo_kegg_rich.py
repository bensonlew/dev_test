# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
# last_modify:20161124
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.objectid import ObjectId
import re


class DenovoKeggRich(Base):
    def __init__(self, bind_object):
        super(DenovoKeggRich, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_kegg_rich(self, name=None, params=None, kegg_enrich_table=None):
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'kegg_enrich' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'params': params,
            'status': 'end',
            'desc': 'kegg富集分析',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        collection = self._db_name['sg_denovo_kegg_enrich']
        enrich_id = collection.insert_one(insert_data).inserted_id
        if kegg_enrich_table:
            self.add_kegg_enrich_detail(enrich_id, kegg_enrich_table)
        self.bind_object.logger.info("add sg_denovo_kegg_enrich sucess!")
        return enrich_id

    def add_kegg_enrich_detail(self, enrich_id, kegg_enrich_table):
        if not isinstance(enrich_id, ObjectId):
            if isinstance(enrich_id, types.StringTypes):
                enrich_id = ObjectId(enrich_id)
            else:
                raise Exception('kegg_enrich_id必须为ObjectId对象或其对应的字符串!')
        if not os.path.exists(kegg_enrich_table):
            raise Exception('kegg_enrich_table所指定的路径:{}不存在，请检查！'.format(kegg_enrich_table))
        data_list = []
        with open(kegg_enrich_table, 'rb') as r:
            for line in r:
                if re.match(r'\w', line):
                    line = line.strip('\n').split('\t')
                    insert_data = {
                        'kegg_enrich_id': enrich_id,
                        'term': line[0],
                        'database': line[1],
                        'id': line[2],
                        'study_number': line[3],
                        'backgroud_number': line[4],
                        'pvalue': line[5],
                        'corrected_pvalue': line[6],
                        'gene_lists': line[7],
                        'hyperlink': line[8]
                    }
                    data_list.append(insert_data)
            try:
                collection = self._db_name['sg_denovo_kegg_enrich_detail']
                collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入kegg富集统计表：%s信息出错:%s" % (kegg_enrich_table, e))
            else:
                self.bind_object.logger.info("导入kegg富集统计表:%s信息成功!" % kegg_enrich_table)
