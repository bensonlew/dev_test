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


class RefAnnotation(Base):
    def __init__(self, bind_object):
        super(RefAnnotation, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    def add_annotation(self, name=None, params=None, ref_anno_path=None, new_anno_path=None, pfam_path=None):
        """
        ref_anno_path: 已知序列注释的结果文件夹
        new_anno_path: 新序列注释的结果文件夹
        pfam_path:转录本的pfam_domain
        """
        new_stat_path = new_anno_path + "/anno_stat/all_annotation_statistics.xls"
        new_venn_path = new_anno_path + "/anno_stat/venn"
        stat_id = self.add_annotation_stat(name=None, params=params, seq_type="new", database="nr,swissprot,pfam,cog,go,kegg")
        if os.path.exists(new_stat_path) and os.path.exists(new_venn_path):
            self.add_annotation_stat_detail(stat_id=stat_id, stat_path=new_stat_path, venn_path=new_venn_path)
        else:
            raise Exception("新序列注释统计文件和venn图文件夹不存在")
        blast_id = self.add_annotation_blast(name=None, params=params, stat_id=stat_id)
        blast_path = new_anno_path + "/anno_stat/blast"
        if os.path.exists(blast_path):
            for db in ["nr", "swissprot"]:
                trans_blast_path = blast_path + "/" + db + '.xls'
                gene_blast_path = blast_path + '/gene_' + db + '.xls'
                self.add_annotation_blast_detail(blast_id=blast_id, seq_type="new", anno_type="transcript", database=db, blast_path=trans_blast_path)
                self.add_annotation_blast_detail(blast_id=blast_id, seq_type="new", anno_type="gene", database=db, blast_path=gene_blast_path)
        else:
            raise Exception("没有blast的结果文件夹")
        nr_id = self.add_annotation_nr(name=None, params=params, stat_id=stat_id)
        nr_path = new_anno_path + "/anno_stat/blast_nr_statistics"
        if os.path.exists(nr_path):
            evalue_path = nr_path + "/nr_evalue.xls"
            similar_path = nr_path + "/nr_similar.xls"
            gene_evalue_path = nr_path + "/gene_nr_evalue.xls"
            gene_similar_path = nr_path + "/gene_nr_similar.xls"
            self.add_annotation_nr_pie(nr_id=nr_id, evalue_path=evalue_path, similar_path=similar_path, seq_type="new", anno_type="transcript")
            self.add_annotation_nr_pie(nr_id=nr_id, evalue_path=gene_evalue_path, similar_path=gene_similar_path, seq_type="new", anno_type="gene")
        else:
            raise Exception("新序列NR注释结果文件不存在")
        swissprot_id = self.add_annotation_swissprot(name=None, params=params, stat_id=stat_id)
        swissprot_path = new_anno_path + "/anno_stat/blast_swissprot_statistics"
        if os.path.exists(swissprot_path):
            evalue_path = swissprot_path + "/swissprot_evalue.xls"
            similar_path = swissprot_path + "/swissprot_similar.xls"
            gene_evalue_path = swissprot_path + "/gene_swissprot_evalue.xls"
            gene_similar_path = swissprot_path + "/gene_swissprot_similar.xls"
            self.add_annotation_swissprot_pie(swissprot_id=swissprot_id, evalue_path=evalue_path, similar_path=similar_path, seq_type="new", anno_type="transcript")
            self.add_annotation_swissprot_pie(swissprot_id=swissprot_id, evalue_path=gene_evalue_path, similar_path=gene_similar_path, seq_type="new", anno_type="gene")
        else:
            raise Exception("新序列Swiss-Prot注释结果文件不存在")
        pfam_id = self.add_annotation_pfam(name=None, params=params, stat_id=stat_id)
        gene_pfam_path = new_anno_path + "/anno_stat/pfam_stat/gene_pfam_domain"
        if os.path.exists(pfam_path) and os.path.exists(gene_pfam_path):
            self.add_annotation_pfam_detail(pfam_id=pfam_id, pfam_path=pfam_path, seq_type="new", anno_type="transcript")
            self.add_annotation_pfam_detail(pfam_id=pfam_id, pfam_path=gene_pfam_path, seq_type="new", anno_type="gene")
            self.add_annotation_pfam_bar(pfam_id=pfam_id, pfam_path=pfam_path, seq_type="new", anno_type="transcript")
            self.add_annotation_pfam_bar(pfam_id=pfam_id, pfam_path=gene_pfam_path, seq_type="new", anno_type="gene")
        else:
            raise Exception("pfam注释结果文件不存在")
        ref_stat_path = ref_anno_path + "/anno_stat/all_annotation_statistics.xls"
        ref_venn_path = ref_anno_path + "/anno_stat/venn"
        if os.path.exists(ref_stat_path) and os.path.exists(ref_venn_path):
            stat_id = self.add_annotation_stat(name=None, params=params, seq_type="ref" , database="cog,go,kegg")
            self.add_annotation_stat_detail(stat_id=stat_id, stat_path=ref_stat_path, venn_path=ref_venn_path)
        else:
            raise Exception("已知序列注释统计文件和venn图文件夹不存在")
        query_id = self.add_annotation_query(name=None, params=params, stat_id=stat_id)
        query_path = ref_anno_path + "/anno_stat/all_annotation_statistics.xls"
        if os.path.exists(query_path):
            self.add_annotation_query_detail(query_id=query_id, query_path=query_path)
        else:
            raise Exception("已知序列注释查询文件all_annotation.xls不存在")
        query_path = new_anno_path + "/anno_stat/all_annotation_statistics.xls"
        if os.path.exists(query_path):
            self.add_annotation_query_detail(query_id=query_id, query_path=query_path)
        else:
            raise Exception("新序列注释查询文件all_annotation.xls不存在")
        cog_id = self.add_annotation_cog(name=name, params=params)
        sum_path = ref_anno_path + "/cog/cog_summary.xls"
        table_path = ref_anno_path + "/cog/cog_table.xls"
        self.add_annotation_cog_detail(cog_id=cog_id, cog_path=sum_path, seq_type="ref", anno_type="transcript")
        self.add_annotation_cog_table(cog_id=cog_id, table_path=table_path, seq_type="ref", anno_type="transcript")
        gene_sum_path = ref_anno_path + "/anno_stat/cog_stat/gene_cog_summary.xls"
        gene_table_path = ref_anno_path + "/anno_stat/cog_stat/gene_cog_table.xls"
        self.add_annotation_cog_detail(cog_id=cog_id, cog_path=gene_sum_path, seq_type="ref", anno_type="gene")
        self.add_annotation_cog_table(cog_id=cog_id, table_path=gene_table_path, seq_type="ref", anno_type="gene")
        sum_path = new_anno_path + "/cog/cog_summary.xls"
        table_path = new_anno_path + "/cog/cog_table.xls"
        self.add_annotation_cog_detail(cog_id=cog_id, cog_path=sum_path, seq_type="new", anno_type="transcript")
        self.add_annotation_cog_table(cog_id=cog_id, table_path=table_path, seq_type="new", anno_type="transcript")
        gene_sum_path = new_anno_path + "/anno_stat/cog_stat/gene_cog_summary.xls"
        gene_table_path = new_anno_path + "/anno_stat/cog_stat/gene_cog_table.xls"
        self.add_annotation_cog_detail(cog_id=cog_id, cog_path=gene_sum_path, seq_type="new", anno_type="gene")
        self.add_annotation_cog_table(cog_id=cog_id, table_path=gene_table_path, seq_type="new", anno_type="gene")

        def add_go(go_id, go_path, gene_go_path, seq_type):
            if os.path.exists(go_path) and os.path.exists(gene_go_path):
                for i in range(2, 5):
                    level = go_path + "/go{}level.xls".format(i)
                    gene_level = gene_go_path + "/gene_go{}level.xls".format(i)
                    self.add_annotation_go_level(go_id=go_id, seq_type=seq_type, anno_type="transcript", level=i, level_path=level)
                    self.add_annotation_go_level(go_id=go_id, seq_type=seq_type, anno_type="gene", level=i, level_path=gene_level)
                stat_level2 = go_path + "/go12level_statistics.xls"
                stat_level3 = go_path + "/go123level_statistics.xls"
                stat_level4 = go_path + "/go124level_statistics.xls"
                gene_stat_level2 = gene_go_path + "/gene_go12level_statistics.xls"
                gene_stat_level3 = gene_go_path + "/gene_go123level_statistics.xls"
                gene_stat_level4 = gene_go_path + "/gene_go1234level_statistics.xls"
                gos_path = go_path + "/query_gos.list"
                gene_gos_path = gene_go_path + "/gene_gos.list"
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="transcript", level=2, go_path=stat_level2)
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="transcript", level=3, go_path=stat_level3)
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="transcript", level=4, go_path=stat_level4)
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="gene", level=2, go_path=gene_stat_level2)
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="gene", level=3, go_path=gene_stat_level3)
                self.add_annotation_go_detail(go_id=go_id, seq_type=seq_type, anno_type="gene", level=4, go_path=gene_stat_level4)
                self.add_annotation_go_list(go_id=go_id, seq_type=seq_type, anno_type="transcript", gos_path=gos_path)
                self.add_annotation_go_list(go_id=go_id, seq_type=seq_type, anno_type="gene", gos_path=gene_gos_path)
            else:
                raise Exception("GO注释的结果文件不存在")
        go_id = self.add_annotation_go(name=name, params=params)
        go_path = ref_anno_path + "/go"
        gene_go_path = ref_anno_path + "/anno_stat/go_stat"
        add_go(go_id=go_id, go_path=go_path, gene_go_path=gene_go_path, seq_type="ref")
        go_path = new_anno_path + "/go"
        gene_go_path = new_anno_path + "/anno_stat/go_stat"
        add_go(go_id=go_id, go_path=go_path, gene_go_path=gene_go_path, seq_type="new")

        def add_kegg(kegg_id, kegg_path, gene_kegg_path, seq_type):
            if os.path.exists(kegg_path) and os.path.exists(gene_kegg_path):
                layer_path = kegg_path + "/kegg_layer.xls"
                pathway_path = kegg_path + "/pathway_table.xls"
                png_path = kegg_path + "/pathways"
                table_path = kegg_path + "/kegg_table.xls"
                gene_layer_path = gene_kegg_path + "/gene_kegg_layer.xls"
                gene_pathway_path = gene_kegg_path + "/gene_pathway_table.xls"
                gene_kegg_path = gene_kegg_path + "/gene_kegg_table.xls"
                gene_png_path = gene_kegg_path + "/gene_pathway"
                self.add_annotation_kegg_categories(kegg_id=kegg_id, seq_type=seq_type, anno_type="transcript", categories_path=layer_path)
                self.add_annotation_kegg_categories(kegg_id=kegg_id, seq_type=seq_type, anno_type="gene", categories_path=gene_layer_path)
                self.add_annotation_kegg_level(kegg_id=kegg_id, seq_type=seq_type, anno_type="transcript", level_path=pathway_path, png_dir=png_path)
                self.add_annotation_kegg_level(kegg_id=kegg_id, seq_type=seq_type, anno_type="gene", level_path=gene_pathway_path, png_dir=gene_png_path)
                self.add_annotation_kegg_table(kegg_id=kegg_id, seq_type=seq_type, anno_type="transcript", table_path=table_path)
                self.add_annotation_kegg_table(kegg_id=kegg_id, seq_type=seq_type, anno_type="gene", table_path=gene_table_path)
            else:
                raise Exception("KEGG注释文件不存在")
        kegg_id = self.add_annotation_kegg(name=None, params=params)
        kegg_path = ref_anno_path + "/kegg"
        gene_kegg_path = ref_anno_path + "/anno_stat/kegg_stat"
        add_kegg(kegg_id=kegg_id, kegg_path=kegg_path, gene_kegg_path=gene_kegg_path, seq_type="ref")
        kegg_path = new_anno_path + "/kegg"
        gene_kegg_path = new_anno_path + "/anno_stat/kegg_stat"
        add_kegg(kegg_id=kegg_id, kegg_path=kegg_path, gene_kegg_path=gene_kegg_path, seq_type="new")

    @report_check
    def add_annotation_stat(self, name=None, params=None, seq_type=None, database=None):
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
            'seq_type': seq_type,
            'database': database
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
                venn_list, gene_venn_list = None, None
                database = ["nr", "swissprot", "pfam", "kegg", "go", "string", "cog"]
                if line[0] in database:
                    venn = venn_path + "/" + line[0] + "_venn.txt"
                    gene_venn = venn_path + "/gene_" + line[0] + "_venn.txt"
                    if os.path.exists(venn) and os.path.exists(gene_venn):
                        with open(venn, "rb") as f:
                            venn_list = f.readline().strip('\n')
                            for line in f:
                                venn_list += ',{}'.format(line.strip('\n'))
                        with open(gene_venn, "rb") as f:
                            gene_venn_list = f.readline().strip('\n')
                            for line in f:
                                gene_venn_list += ',{}'.format(line.strip('\n'))
                        data.append(("gene_list", venn_list))
                        data.append(("transcript_list", gene_venn_list))
                    else:
                        raise Exception("{}对应的venn.txt文件不存在".format(line[0]))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_stat_detail']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入注释统计信息失败:%s" % stat_path)
        else:
            self.bind_object.logger.info("导入注释统计信息成功：%s" % (stat_path))

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
            ('gene', len(gene_nr_ids)),
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
            ('gene', len(gene_sw_ids)),
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
            raise Exception("导入注释统计信息出错")
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

    @report_check
    def add_annotation_blast(self, name=None, params=None, stat_id=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationBlast_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'blast最佳比对结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stat_id': stat_id
        }
        collection = self.db['sg_annotation_blast']
        blast_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_blast!")
        return blast_id

    @report_check
    def add_annotation_blast_detail(self, blast_id, seq_type, anno_type, database, blast_path):
        if not isinstance(blast_id, ObjectId):
            if isinstance(blast_id, types.StringTypes):
                blast_id = ObjectId(blast_id)
            else:
                raise Exception('blast_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(blast_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(blast_path))
        data_list = []
        with open(blast_path, 'r') as f:
            lines = f.readlines()
            flag, hit = None, None
            for line in lines[1:]:
                line = line.strip().split('\t')
                query_name = line[5]
                hit_name = line[10]
                if flag == query_name:
                    if hit == hit_name:
                        pass
                    else:
                        flag = query_name
                        hit = hit_name
                        data = {
                            'blast_id': blast_id,
                            'seq_type': seq_type,
                            'anno_type': anno_type,
                            'database': database,
                            'score': float(line[0]),
                            'e_value': float(line[1]),
                            'hsp_len': int(line[2]),
                            'identity_rate': round(float(line[3]), 4),
                            'similarity_rate': round(float(line[4]), 4),
                            'query_id': line[5],
                            'q_len': int(line[6]),
                            'q_begin': line[7],
                            'q_end': line[8],
                            'q_frame': line[9],
                            'hit_name': line[10],
                            'hit_len': int(line[11]),
                            'hsp_begin': line[12],
                            'hsp_end': line[13],
                            'hsp_frame': line[14],
                            'description': line[15]
                        }
                        collection = self.db['sg_annotation_blast_detail']
                        collection.insert_one(data).inserted_id
                else:
                    flag = query_name
                    hit = hit_name
                    data = {
                        'blast_id': blast_id,
                        'seq_type': seq_type,
                        'anno_type': anno_type,
                        'database': database,
                        'score': float(line[0]),
                        'e_value': float(line[1]),
                        'hsp_len': int(line[2]),
                        'identity_rate': round(float(line[3]), 4),
                        'similarity_rate': round(float(line[4]), 4),
                        'query_id': line[5],
                        'q_len': int(line[6]),
                        'q_begin': line[7],
                        'q_end': line[8],
                        'q_frame': line[9],
                        'hit_name': line[10],
                        'hit_len': int(line[11]),
                        'hsp_begin': line[12],
                        'hsp_end': line[13],
                        'hsp_frame': line[14],
                        'description': line[15]
                    }
                    collection = self.db['sg_annotation_blast_detail']
                    collection.insert_one(data).inserted_id
        self.bind_object.logger.info("导入blast信息：%s成功!" % (blast_path))
        # try:
        #     collection = self.db['sg_annotation_blast_detail']
        #     collection.insert_many(data_list)
        # except Exception, e:
        #     raise Exception("导入blast信息：%s出错!" % (blast_path))
        # else:
        #     self.bind_object.logger.info("导入blast信息：%s成功!" % (blast_path))

    @report_check
    def add_annotation_nr(self, name=None, params=None, stat_id=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationNr_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'nr注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stat_id': stat_id
        }
        collection = self.db['sg_annotation_nr']
        nr_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add sg_annotation_nr!")
        return nr_id

    @report_check
    def add_annotation_nr_pie(self, nr_id, evalue_path, similar_path, seq_type, anno_type):
        if not isinstance(nr_id, ObjectId):
            if isinstance(nr_id, types.StringTypes):
                nr_id = ObjectId(nr_id)
            else:
                raise Exception('nr_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(evalue_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(evalue_path))
        if not os.path.exists(similar_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(similar_path))
        evalue, evalue_list, similar, similar_list = [], [], [], []
        data_list = []
        with open(evalue_path, "r") as f1, open(similar_path, "r") as f2:
            lines1 = f1.readlines()
            lines2 = f2.readlines()
            for line1 in lines1[1:]:
                line1 = line1.strip().split('\t')
                value = {"key": line1[0], "value": int(line1[1])}
                try:
                    value_list = {"key": line1[0], "value": line1[2]}
                except:
                    value_list = {"key": line1[0], "value": None}
                evalue.append(value)
                evalue_list.append(value_list)
            for line2 in lines2[1:]:
                line2 = line2.strip().split('\t')
                similarity = {"key": line2[0], "value": int(line2[1])}
                try:
                    similarity_list = {"key": line2[0], "value": line2[2]}
                except:
                    similarity_list = {"key": line2[0], "value": None}
                similar.append(similarity)
                similar_list.append(similarity_list)
        data = [
            ('nr_id', nr_id),
            ('seq_type', seq_type),
            ('anno_type', anno_type),
            ('e_value', evalue),
            ('similar', similar),
            ('evalue_list', evalue_list),
            ('similar_list', similar_list),
        ]
        data = SON(data)
        data_list.append(data)
        try:
            collection = self.db['sg_annotation_nr_pie']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入nr库注释作图信息evalue,similar：%s、%s出错!" % (evalue_path, similar_path))
        else:
            self.bind_object.logger.info("导入nr库注释作图信息evalue,similar：%s、%s成功!" % (evalue_path, similar_path))

    @report_check
    def add_annotation_swissprot(self, name=None, params=None, stat_id=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationSwissprot_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'swissprot注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stat_id': stat_id
        }
        collection = self.db['sg_annotation_swissprot']
        swissprot_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add sg_annotation_swissprot!")
        return swissprot_id

    def add_annotation_swissprot_pie(self, swissprot_id, evalue_path, similar_path, seq_type, anno_type):
        """
        """
        if not isinstance(swissprot_id, ObjectId):
            if isinstance(swissprot_id, types.StringTypes):
                swissprot_id = ObjectId(swissprot_id)
            else:
                raise Exception('swissprot_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(evalue_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(evalue_path))
        if not os.path.exists(similar_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(similar_path))
        evalue, evalue_list, similar, similar_list = [], [], [], []
        data_list = []
        with open(evalue_path, "r") as f1, open(similar_path, "r") as f2:
            lines1 = f1.readlines()
            lines2 = f2.readlines()
            for line1 in lines1[1:]:
                line1 = line1.strip().split('\t')
                value = {"key": line1[0], "value": int(line1[1])}
                try:
                    value_list = {"key": line1[0], "value": line1[2]}
                except:
                    value_list = {"key": line1[0], "value": None}
                evalue.append(value)
                evalue_list.append(value_list)
            for line2 in lines2[1:]:
                line2 = line2.strip().split('\t')
                similarity = {"key": line2[0], "value": int(line2[1])}
                try:
                    similarity_list = {"key": line2[0], "value": line2[2]}
                except:
                    similarity_list = {"key": line2[0], "value": None}
                similar.append(similarity)
                similar_list.append(similarity_list)
        data = [
            ('swissprot_id', swissprot_id),
            ('seq_type', seq_type),
            ('anno_type', anno_type),
            ('e_value', evalue),
            ('similar', similar),
            ('evalue_list', evalue_list),
            ('similar_list', similar_list),
        ]
        data = SON(data)
        data_list.append(data)
        try:
            collection = self.db['sg_annotation_swissprot_pie']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入swissprot库注释作图信息evalue,similar：%s、%s出错!" % (evalue_path, similar_path))
        else:
            self.bind_object.logger.info("导入swissprot库注释作图信息evalue,similar：%s、%s成功!" % (evalue_path, similar_path))

    @report_check
    def add_annotation_pfam(self, name=None, params=None, stat_id=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if not isinstance(stat_id, ObjectId):
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationPfam_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'pfam注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stat_id': stat_id
        }
        collection = self.db['sg_annotation_pfam']
        pfam_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add sg_annotation_pfam!")
        return pfam_id

    def add_annotation_pfam_detail(self, pfam_id, pfam_path, seq_type, anno_type):
        """
        pfam_path: pfam_domain
        """
        if not isinstance(pfam_id, ObjectId):
            if isinstance(pfam_id, types.StringTypes):
                pfam_id = ObjectId(pfam_id)
            else:
                raise Exception('pfam_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(pfam_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(pfam_path))
        data_list = []
        with open(pfam_path, "r") as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ('pfam_id', pfam_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('seq_id', line[0]),
                    ('pfam', line[2]),
                    ('domain', line[3]),
                    ('description', line[4])
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_pfam_detail']
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.info("导入pfam注释信息:%s失败！" % pfam_path)
        else:
            self.bind_object.logger.info("导入pfam注释信息:%s成功" % pfam_path)

    @report_check
    def add_annotation_pfam_bar(self, pfam_id, pfam_path, seq_type, anno_type):
        pfam = []
        domain = {}
        with open(pfam_path, "rb") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip().split("\t")
                if line[3] not in pfam:
                    pfam.append(line[3])
                    domain[line[3]] = 1
                else:
                    domain[line[3]] += 1
        if not isinstance(pfam_id, ObjectId):
            if isinstance(pfam_id, types.StringTypes):
                pfam_id = ObjectId(pfam_id)
            else:
                raise Exception('pfam_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(pfam_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(pfam_path))
        data_list = []
        for i in domain:
            data = [
                ('pfam_id', pfam_id),
                ('seq_type', seq_type),
                ('anno_type', anno_type),
                ('domain', i),
                ('num', domain[i])
            ]
            data = SON(data)
            data_list.append(data)
        try:
            collection = self.db['sg_annotation_pfam_bar']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入pfam注释信息:%s失败！" % pfam_path)
        else:
            self.bind_object.logger.info("导入pfam注释信息:%s成功！" % pfam_path)

    @report_check
    def add_annotation_cog(self, name=None, params=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationCog_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'cog注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        collection = self.db['sg_annotation_cog']
        cog_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_cog!")
        return cog_id

    @report_check
    def add_annotation_cog_detail(self, cog_id, cog_path, seq_type, anno_type):
        '''
        cog_path: cog_summary.xls
        seq_type: ref/new
        anno_type: transcript/gene
        '''
        if not isinstance(cog_id, ObjectId):
            if isinstance(cog_id, types.StringTypes):
                cog_id = ObjectId(cog_id)
            else:
                raise Exception('cog_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cog_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(cog_path))
        data_list = list()
        with open(cog_path, 'r') as f:
            lines = f.readlines()
            for line in lines[2:]:
                line = line.strip().split('\t')
                data = [
                    ('cog_id', cog_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('type', line[0]),
                    ('function_categories', line[1]),
                    ('cog', int(line[2])),
                    ('nog', int(line[3])),
                    ('kog', int(line[4]))
                ]
                try:
                    data.append(('cog_list', line[5]))
                except:
                    data.append(('cog_list', None))
                try:
                    data.append(('nog_list', line[6]))
                except:
                    data.append(('nog_list', None))
                try:
                    data.append(('kog_list', line[7]))
                except:
                    data.append(('kog_list', None))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_cog_detail']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入cog注释信息：%s出错!" % (cog_path))
        else:
            self.bind_object.logger.info("导入cog注释信息：%s成功!" % (cog_path))

    def add_annotation_cog_table(self, cog_id, table_path, seq_type, anno_type):
        '''
        table_path:cog_table.xls
        '''
        if not isinstance(cog_id, ObjectId):
            if isinstance(cog_id, types.StringTypes):
                cog_id = ObjectId(cog_id)
            else:
                raise Exception('cog_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(table_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(table_path))
        data_list = list()
        with open(table_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('cog_id', cog_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('query_name', line[0]),
                    ('query_length', line[1]),
                    ('hsp_start', line[2]),
                    ('hsp_end', line[3]),
                    ('hsp_strand', line[4]),
                    ('hit_name', line[5]),
                    ('hit_description', line[6]),
                    ('hit_length', line[7]),
                    ('hit_start', line[8]),
                    ('hit_end', line[9]),
                    ('group', line[10]),
                    ('group_description', line[11]),
                    ('group_categories', line[12]),
                    ('region_start', line[13]),
                    ('region_end', line[14]),
                    ('region_coverage', line[15]),
                    ('region_identities', line[16]),
                    ('region_positives', line[17])
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_cog_table']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入cog注释table信息：%s出错!" % (table_path))
        else:
            self.bind_object.logger.info("导入cog注释table信息：%s成功!" % (table_path))

    @report_check
    def add_annotation_go(self, name=None, params=None):
        """
        go注释导表函数
        """
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationGo_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'go注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        collection = self.db['sg_annotation_go']
        go_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_go!")
        return go_id

    def add_annotation_go_detail(self, go_id, seq_type, anno_type, level, go_path):
        """
        go_path: go1234level_statistics.xls/go123level_statistics.xls/go12level_statistics.xls
        """
        if not isinstance(go_id, ObjectId):
            if isinstance(go_id, types.StringTypes):
                go_id = ObjectId(go_id)
            else:
                raise Exception('go_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(go_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(go_path))
        data_list = list()
        with open(go_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('go_id', go_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('level', level),
                    ('goterm', line[0]),
                    ('goterm_2', line[1]),
                    ('goid_2', line[2]),
                    ('seq_number', int(line[-2])),
                    ('percent', round(line[-2], 4)),
                    ('seq_list', line[-1])
                ]
                if level >= 3:
                    data.append(('goterm_3', line[3]))
                    data.append(('goid_3', line[4]))
                if level == 4:
                    data.append(('goterm_4', line[5]))
                    data.append(('goid_4', line[6]))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_go_detail']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入go注释信息：%s出错!" % (go_path))
        else:
            self.bind_object.logger.info("导入go注释信息：%s成功!" % (go_path))

    @report_check
    def add_annotation_go_level(self, go_id, seq_type, anno_type, level, level_path):
        """
        level_path: go2level.xls
        """
        if not isinstance(go_id, ObjectId):
            if isinstance(go_id, types.StringTypes):
                go_id = ObjectId(go_id)
            else:
                raise Exception('go_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(level_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(level_path))
        data_list = list()
        with open(level_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('go_id', go_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('level', level),
                    ('term_type', line[1]),
                    ('parent_name', line[0]),
                    ('num', int(line[3])),
                    ('percent', round(float(line[4]), 4)),
                    ('go', line[2]),
                    ('seq_list', line[5]),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_go_level']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入go注释第二层级信息：%s出错!" % (level_path))
        else:
            self.bind_object.logger.info("导入go注释第二层级信息：%s成功!" % (level_path))

    @report_check
    def add_annotation_go_list(self, go_id, seq_type, anno_type, gos_path):
        """
        gos_path: go.list
        """
        if not isinstance(go_id, ObjectId):
            if isinstance(go_id, types.StringTypes):
                go_id = ObjectId(go_id)
            else:
                raise Exception('go_id须为ObjectId对象或其他对应的字符串！')
        if not os.path.exists(gos_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(gos_path))
        data_list = []
        with open(gos_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip().split('\t')
                data = [
                    ('go_id', go_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('gene_id', line[0]),
                    ('gos_list', line[1]),
                ]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self.db['sg_annotation_go_list']
                collection.insert_many(data_list)
            except Exception, e:
                raise Exception("导入gos_list注释信息：%s出错:%s" % (gos_path))
            else:
                self.bind_object.logger.info("导入gos_list注释信息：%s成功!" % (gos_path))

    @report_check
    def add_annotation_kegg(self, name=None, params=None):
        """
        kegg注释导表函数
        """
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationKegg_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': 'kegg注释结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        collection = self.db['sg_annotation_kegg']
        kegg_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_kegg!")
        return kegg_id

    @report_check
    def add_annotation_kegg_categories(self, kegg_id, seq_type, anno_type, categories_path):
        """
        categories_path:kegg_layer.xls
        """
        if not isinstance(kegg_id, ObjectId):
            if isinstance(kegg_id, types.StringTypes):
                kegg_id = ObjectId(kegg_id)
            else:
                raise Exception('kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(categories_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(categories_path))
        data_list = list()
        with open(categories_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip().split('\t')
                data = [
                    ('kegg_id', kegg_id),
                    ('seq_type', seq_type),
                    ('anno_type', anno_type),
                    ('first_category', line[0]),
                    ('second_category', line[1]),
                    ('num', int(line[2])),
                    ('seq_list', line[3]),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_kegg_categories']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入kegg注释分类信息：%s出错!" % (categories_path))
        else:
            self.bind_object.logger.info("导入kegg注释分类信息：%s 成功!" % categories_path)

    @report_check
    def add_annotation_kegg_level(self, kegg_id, seq_type, anno_type, level_path, png_dir):
        """
        level_path: pathway_table.xls
        """
        if not isinstance(kegg_id, ObjectId):
            if isinstance(kegg_id, types.StringTypes):
                kegg_id = ObjectId(kegg_id)
            else:
                raise Exception('kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(level_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(level_path))
        if not os.path.exists(png_dir):
            raise Exception('{}所指定的路径不存在，请检查！'.format(png_dir))
        data_list = []
        with open(level_path, 'rb') as r:
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                fs = gridfs.GridFS(self.db)
                pid = re.sub('path:', '', line[0])
                pdfid = fs.put(open(png_dir + '/' + pid + '.pdf', 'rb'))
                graph_png_id = fs.put(open(png_dir + '/' + pid + '.png', 'rb'))
                insert_data = {
                    'kegg_id': kegg_id,
                    'seq_type': seq_type,
                    'anno_type': anno_type,
                    'pathway_id': line[0],
                    'first_category': line[1],
                    'second_category': line[2],
                    'pathway_definition': line[3],
                    'number_of_seqs': int(line[4]),
                    'seq_list': line[5],
                    'graph_id': pdfid,
                    'graph_png_id': graph_png_id
                }
                data_list.append(insert_data)
        try:
            collection = self.db['sg_annotation_kegg_level']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入kegg注释层级信息：%s、%s出错!" % (level_path, png_dir))
        else:
            self.bind_object.logger.info("导入kegg注释层级信息：%s、%s 成功!" % (level_path, png_dir))

    @report_check
    def add_annotation_kegg_table(self, kegg_id, seq_type, anno_type, table_path):
        if not isinstance(kegg_id, ObjectId):
            if isinstance(kegg_id, types.StringTypes):
                kegg_id = ObjectId(kegg_id)
            else:
                raise Exception('kegg_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(table_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(table_path))
        with open(table_path, 'rb') as r:
            data_list = []
            r.readline()
            for line in r:
                line = line.strip('\n').split('\t')
                insert_data = {
                    'kegg_id': kegg_id,
                    'seq_type': seq_type,
                    'anno_type': anno_type,
                    'query_id': line[0],
                    'ko_id': line[1],
                    'ko_name': line[2],
                    'hyperlink': line[3],
                    'paths': line[4],
                }
                data_list.append(insert_data)
        try:
            collection = self.db['sg_annotation_kegg_table']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入kegg注释table信息：%s出错!" % (table_path))
        else:
            self.bind_object.logger.info("导入kegg注释table信息：%s成功!" % (table_path))

    @report_check
    def add_annotation_query(self, name=None, params=None, stat_id=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        if not isinstance(stat_id, ObjectId):
            print stat_id
            if isinstance(stat_id, types.StringTypes):
                stat_id = ObjectId(stat_id)
            else:
                raise Exception('stat_id必须为ObjectId对象或其对应的字符串！')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'AnnotationQuery_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': '注释查询主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stat_id': stat_id
        }
        collection = self.db['sg_annotation_query']
        query_id = collection.insert_one(insert_data).inserted_id
        self.bind_object.logger.info("add ref_annotation_query!")
        return query_id

    @report_check
    def add_annotation_query_detail(self, query_id, query_path):
        if not isinstance(query_id, ObjectId):
            if isinstance(query_id, types.StringTypes):
                query_id = ObjectId(query_id)
            else:
                raise Exception('query_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(query_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(query_path))
        data_list = []
        with open(query_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('query_id', query_id),
                    ('transcript_id', line[0]),
                    ('gene_id', line[1]),
                ]
                try:
                    data.append(('gene_name', line[2]))
                except:
                    data.append(('gene_name', None))
                try:
                    data.append(('length', line[3]))
                except:
                    data.append(('length', None))
                try:
                    data.append(('cog', line[4]))
                    data.append(('cog_description', line[7]))
                except:
                    data.append(('cog', None))
                    data.append(('cog_description', None))
                try:
                    data.append(('nog', line[5]))
                    data.append(('nog_description', line[8]))
                except:
                    data.append(('nog', None))
                    data.append(('nog_description', None))
                try:
                    data.append(('kog', line[6]))
                    data.append(('kog_description', line[9]))
                except:
                    data.append(('kog', None))
                    data.append(('kog_description', None))
                try:
                    data.append(('ko_id', line[10]))
                    data.append(('ko_name', line[11]))
                except:
                    data.append(('ko_id', None))
                    data.append(('ko_name', None))
                try:
                    data.append(('pathways', line[12]))
                except:
                    data.append(('pathways', None))
                try:
                    data.append(('pfam', line[13]))
                except:
                    data.append(('pfam', None))
                try:
                    data.append(('go', line[14]))
                except:
                    data.append(('go', None))
                try:
                    data.append(('nr', line[15]))
                except:
                    data.append(('nr', None))
                try:
                    data.append(('swissprot', line[16]))
                except:
                    data.append(('swissprot', None))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_annotation_query_detail']
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入注释统计信息：%s出错!" % (query_path))
        else:
            self.bind_object.logger.info("导入注释统计信息：%s成功!" % (query_path))
