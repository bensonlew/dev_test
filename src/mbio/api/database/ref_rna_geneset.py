# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
# last_modify:20161205

import os
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import json
import re


class RefRnaGeneset(Base):
    def __init__(self, bind_object):
        super(RefRnaGeneset, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @report_check
    def add_geneset_cog_detail(self, geneset_cog_table, geneset_cog_id=None):
        data_list = []
        with open(geneset_cog_table, 'r') as f:
            first_line = f.readline().strip().split("\t")
            print first_line
            geneset_name = []
            for gn in first_line[2:]:
                if not gn[:-4] in geneset_name:
                    geneset_name.append(gn[:-4])
            print geneset_name
            for line in f:
                line = line.strip().split("\t")
                data = {
                    'geneset_cog_id': geneset_cog_id,
                    'type': line[0],
                    'function_categories': line[1]
                }
                for n, gn in enumerate(geneset_name):
                    data[gn + "_cog"] = int(line[n+2])
                    data[gn + "_nog"] = int(line[n+2])
                    data[gn + "_kog"] = int(line[n+2])
                # print data
                # data = SON(data)
                data_list.append(data)
            try:
                collection = self.db['sg_geneset_cog_class_detail']
                collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入cog表格：%s出错:%s" % (geneset_cog_table, e))
            else:
                self.bind_object.logger.error("导入cog表格：%s成功!" % (geneset_cog_table))

    @report_check
    def add_go_enrich_detail(self, go_enrich_id, go_enrich_dir):
        if not isinstance(go_enrich_id, ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(go_enrich_dir):
            raise Exception('{}所指定的路径不存在。请检查！'.format(go_enrich_dir))
        data_list = []
        with open(go_enrich_dir, 'r') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                if float(line[8]):
                    m = re.match(r"(.+)/(.+)", line[5])
                    pop_count = int(m.group(1))
                    line[6] = float(line[6])
                    line[7] = int(line[7])
                    line[8] = int(line[8])
                    line[9] = float(line[9])
                    line[10] = float(line[10])
                    line[11] = float(line[11])
                    line[12] = float(line[12])
                    data = [
                        ('go_enrich_id', go_enrich_id),
                        ('go_id', line[0]),
                        ('go_type', line[1]),
                        ('enrichment', line[2]),
                        ('discription', line[3]),
                        ('ratio_in_study', line[4]),
                        ('ratio_in_pop', line[5]),
                        ('p_uncorrected', line[6]),
                        ('depth', line[7]),
                        ('study_count', line[8]),
                        ('pop_count', pop_count),
                        ('gene_list', line[13]),
                    ]
                    try:
                        data += [('p_bonferroni', line[9])]
                    except:
                        data += [('p_bonferroni', '')]
                    try:
                        data += [('p_sidak', line[10])]
                    except:
                        data += [('p_sidak', '')]
                    try:
                        data += [('p_holm', line[11])]
                    except:
                        data += [('p_holm', '')]
                    try:
                        data += [('p_fdr', line[12])]
                    except:
                        data += [('p_fdr', '')]
                    data = SON(data)
                    data_list.append(data)
        try:
            collection = self.db['sg_geneset_go_enrich_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print("导入go富集信息：%s出错:%s" % (go_enrich_dir, e))
        else:
            print("导入go富集信息：%s成功!" % (go_enrich_dir))

    @report_check
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
                        'study_number': int(line[3]),
                        'backgroud_number': int(line[4]),
                        'pvalue': round(float(line[5]), 4),
                        'corrected_pvalue': round(float(line[6]), 4),
                        'gene_lists': line[7],
                        'hyperlink': line[8]
                    }
                    data_list.append(insert_data)
            if data_list:
                try:
                    collection = self.db['sg_geneset_kegg_enrich_detail']
                    collection.insert_many(data_list)
                except Exception, e:
                    self.bind_object.logger.error("导入kegg富集统计表：%s信息出错:%s" % (kegg_enrich_table, e))
                else:
                    self.bind_object.logger.info("导入kegg富集统计表:%s信息成功!" % kegg_enrich_table)
            else:
                coll = self.db['sg_geneset_kegg_enrich']
                coll.update({'_id': enrich_id}, {'$set': {'desc': 'no_result'}})
                self.bind_object.logger.info("kegg富集统计表没结果：" % kegg_enrich_table)
