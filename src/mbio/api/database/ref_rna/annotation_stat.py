# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import os
import re
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
import gridfs
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class AnnotationStat(Base):
    def __init__(self, bind_object):
        super(AnnotationStat, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @report_check
    def add_annotation_stat(self, name=None, params=None, seq_type=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationStat_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': '注释统计主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'seq_type': seq_type
        }
        collection = self.db['sg_annotation_stat']
        stat_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_stat!")
        return stat_id

    @report_check
    def add_annotation_stat_detail(self, stat_id, stat_path, venn_path):
        """
        database: 进行统计的数据库
        stat_path: all_annotation_statistics.xls
        venn_path: venn图目录
        """
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(stat_path))
        if not os.path.exists(venn_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(venn_path))
        nr_venn = venn_path + "/nr_venn.txt"
        gene_nr_venn = venn_path + "/gene_nr_venn.txt"
        cog_venn = venn_path + "/cog_venn.txt"
        gene_cog_venn = venn_path + "/gene_cog_venn.txt"
        swissprot_venn = venn_path + "/swissprot_venn.txt"
        gene_swissprot_venn = venn_path + "/gene_swissprot_venn.txt"
        kegg_venn = venn_path + "/kegg_venn.txt"
        gene_kegg_venn = venn_path + "/gene_kegg_venn.txt"
        data_list = []
        with open(stat_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('stat_id', stat_id),
                    ('type', line[0]),
                    ('transcript', int(line[1])),
                    ('gene', int(line[2])),
                    ('transcript_percent', round(float(line[3]), 4)),
                    ('gene_percent', round(float(line[4]), 4)),
                ]
                if line[0] == "nr":
                    with open(nr_venn, "rb") as f:
                        venn_list = f.readline().strip('\n')
                        for line in f:
                            venn_list += ',{}'.format(line.strip('\n'))
                    with open(gene_nr_venn, "rb") as f:
                        gene_venn_list = f.readline().strip('\n')
                        for line in f:
                            gene_venn_list += ',{}'.format(line.strip('\n'))
                if line[0] == "cog":
                    with open(cog_venn, "rb") as f:
                        venn_list = f.readline().strip('\n')
                        for line in f:
                            venn_list += ',{}'.format(line.strip('\n'))
                    with open(gene_cog_venn, "rb") as f:
                        gene_venn_list = f.readline().strip('\n')
                        for line in f:
                            gene_venn_list += ',{}'.format(line.strip('\n'))
                if line[0] == "swissprot":
                    with open(swissprot_venn, "rb") as f:
                        venn_list = f.readline().strip('\n')
                        for line in f:
                            venn_list += ',{}'.format(line.strip('\n'))
                    with open(gene_swissprot_venn, "rb") as f:
                        gene_venn_list = f.readline().strip('\n')
                        for line in f:
                            gene_venn_list += ',{}'.format(line.strip('\n'))
                if line[0] == "kegg":
                    with open(kegg_venn, "rb") as f:
                        venn_list = f.readline().strip('\n')
                        for line in f:
                            venn_list += ',{}'.format(line.strip('\n'))
                    with open(gene_kegg_venn, "rb") as f:
                        gene_venn_list = f.readline().strip('\n')
                        for line in f:
                            gene_venn_list += ',{}'.format(line.strip('\n'))
                data.append(("gene_list", venn_list))
                data.append(("transcript_list", gene_venn_list))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_stat_detail']
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入注释统计信息：%s出错!" % (stat_path))
        else:
            self.bind_object.logger.info("导入注释统计信息：%s成功!" % (stat_path))

    @report_check
    def add_stat_detail(self, old_stat_id, stat_id, nr_evalue, gene_nr_evalue, sw_evalue, gene_sw_evalue):
        """
        注释重运行时注释统计导表sg_annotation_stat_detail
        """
        if not isinstance(old_stat_id, ObjectId):
            if isinstance(old_stat_id, types.StringTypes):
                old_stat_id = ObjectId(old_stat_id)
            else:
                raise Exception('old_stat_id必须为ObjectId对象或其对应的字符串！')
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        collection = self.db["sg_annotation_stat_detail"]
        results = collection.find({"stat_id": old_stat_id})
        data_list, data = [], []
        for result in results:
            db = result["type"]
            if db == "total":
                total_tran = result["transcript"]
                total_gene = result["gene"]
            if db in ["pfam", "total_anno", "total"]:
                data = [
                    ('stat_id', stat_id),
                    ('type', result["type"]),
                    ('transcript', result["transcript"]),
                    ('gene', result["gene"]),
                    ('transcript_percent', result["transcript_percent"]),
                    ('gene_percent', result["gene_percent"]),
                    ('gene_list', result["gene_list"]),
                    ('transcript_list', result["transcript_list"])
                ]
                data = SON(data)
                data_list.append(data)
        nr_ids = self.stat(stat_path=nr_evalue)
        gene_nr_ids = self.stat(stat_path=gene_nr_evalue)
        data = [
            ('stat_id', stat_id),
            ('type', "nr"),
            ('transcript', len(nr_ids)),
            ('gene',len(gene_nr_ids)),
            ('transcript_percent', round(float(len(nr_ids))/total_tran, 4)),
            ('gene_percent', round(float(len(gene_nr_ids))/total_gene, 4)),
            ('gene_list', ";".join(gene_nr_ids)),
            ('transcript_list', ";".join(nr_ids))
        ]
        data = SON(data)
        data_list.append(data)
        sw_ids = self.stat(stat_path=sw_evalue)
        gene_sw_ids = self.stat(stat_path=gene_sw_evalue)
        data = [
            ('stat_id', stat_id),
            ('type', "swissprot"),
            ('transcript', len(sw_ids)),
            ('gene',len(gene_sw_ids)),
            ('transcript_percent', round(float(len(sw_ids))/total_tran, 4)),
            ('gene_percent', round(float(len(gene_sw_ids))/total_gene, 4)),
            ('gene_list', ";".join(gene_sw_ids)),
            ('transcript_list', ";".join(sw_ids))
        ]
        data = SON(data)
        data_list.append(data)
        try:
            collection = self.db['sg_annotation_stat_detail']
            collection.insert_many(data_list)
        except:
            self.bind_object.logger.error("导入注释统计信息出错")
        else:
            self.bind_object.logger.info("导入注释统计信息成功")

    def stat(self, stat_path):
        with open(stat_path, "rb") as f:
            id_list = []
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split("\t")
                try:
                    ids = line[2].split(";")
                except:
                    ids = []
                for i in ids:
                    if i not in id_list:
                        id_list.append(i)
        return id_list
