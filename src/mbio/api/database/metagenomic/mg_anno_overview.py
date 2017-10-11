# -*- coding: utf-8 -*-
# __author__ = 'shaohua.yuan'
# last_modify:20170930
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class MgAnnoOverview(Base):
    def __init__(self, bind_object):
        super(MgAnnoOverview, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_anno_overview(self, geneset_name):
        # 主表, 所有的函数名称以add开头，里面可以加需要导入数据库而表格里没有的信息作为参数
        if not isinstance(geneset_name, ObjectId):  # 检查传入的anno_overview_id是否符合ObjectId类型
            if isinstance(geneset_name, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                geneset_name = ObjectId(geneset_name)
            else:  # 如果是其他类型，则报错
                raise Exception('argset_name必须为ObjectId对象或其对应的字符串！')
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': 'overview注释主表',
            'created_ts': created_ts,
            'name': 'null',
            'params': 'null',
            'status': 'end',
            'geneset_name': geneset_name,
        }
        collection = self.db['anno_overview']
        # 将主表名称写在这里
        anno_overview_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        return anno_overview_id

    @report_check
    # def add_anno_overview_detail(self, anno_overview_id, gene_profile, nr = nr_anno, cog = cog_anno, kegg = kegg_anno, ardb = ardb_ano, card = card_anno, cazy = cazy_anno, vfdb = vfdb_anno):
    def add_anno_overview_detail(self, anno_overview_id, gene_profile, nr=None, cog=None, kegg=None, ardb=None,
                                 card=None, cazy=None, vfdb=None):
        if not isinstance(anno_overview_id, ObjectId):  # 检查传入的anno_vfdb_id是否符合ObjectId类型
            if isinstance(anno_overview_id, types.StringTypes):  # 如果是string类型，则转化为ObjectId
                anno_overview_id = ObjectId(anno_overview_id)
            else:  # 如果是其他类型，则报错
                raise Exception('anno_overview_id必须为ObjectId对象或其对应的字符串！')
        anno_files = [nr, cog, kegg, cazy, ardb, card, vfdb]
        if not os.path.exists(gene_profile):
            raise Exception('gene_profile所指定的路径不存在，请检查！')
        for i in anno_files:
            if i != None:
                if not os.path.exists(i):  # 检查要上传的数据表路径是否存在
                    raise Exception(i + '所指定的路径不存在，请检查！')
        with open(gene_profile, "rb") as f:
            for line in f:
                line = line.strip().split('\t')
                if not line[0] == "GeneID":
                    gene = line[0]
                    insert_data = {
                            'anno_overview_id':anno_overview_id,
                            'query': gene
                                   }
                    if nr != None:
                        insert_data["nr_taxid"] = "--"
                        insert_data["nr_identity"] = "--"
                        insert_data["nr_align_length"] = "--"
                    if cog != None:
                        insert_data["cog_id"] = "--"
                        insert_data["cog_identity"] = "--"
                        insert_data["cog_align_length"] = "--"
                    if kegg != None:
                        insert_data["kegg_gene"] = "--"
                        insert_data["kegg_KO"] = "--"
                        insert_data["kegg_identity"] = "--"
                        insert_data["kegg_align_length"] = "--"
                    if cazy != None:
                        insert_data["cazy_family"] = "--"
                        insert_data["cazy_identity"] = "--"
                        insert_data["cazy_align_length"] = "--"
                    if card != None:
                        insert_data["card_aro"] = "--"
                        insert_data["card_identity"] = "--"
                        insert_data["card_align_length"] = "--"
                    if vfdb != None:
                        insert_data["vfdb_vfs"] = "--"
                        insert_data["vfdb_identity"] = "--"
                        insert_data["vfdb_align_length"] = "--"
                    collection = self.db['anno_overview_detail']
                    collection.insert_one(insert_data)
            collection.ensure_index('query', unique=False)
        if nr != None:
            with open(nr, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        nr_taxid = line[head.index("Taxid")]
                        nr_identity = line[head.index("Identity(%)")]
                        nr_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'nr_taxid': nr_taxid, 'nr_identity': nr_identity,
                                                                     'nr_align_length': nr_align_length}})
        if cog != None:
            with open(cog, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        cog_id = line[head.index("NOG")]
                        cog_identity = line[head.index("Identity(%)")]
                        cog_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'cog_id': cog_id,
                                                                     'cog_identity': cog_identity,
                                                                     'cog_align_length': cog_align_length}})
        if kegg != None:
            with open(kegg, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        kegg_gene = line[head.index("Gene")]
                        kegg_orthology = line[head.index("KO")]
                        kegg_identity = line[head.index("Identity(%)")]
                        kegg_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {
                            'kegg_gene': kegg_gene, 'kegg_KO': kegg_orthology,
                            'kegg_identity': kegg_identity,
                            'kegg_align_length': kegg_align_length}})
        if cazy != None:
            with open(cazy, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        cazy_family = line[head.index("Family")]
                        cazy_identity = line[head.index("Identity(%)")]
                        cazy_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'cazy_family': cazy_family,
                                                    'cazy_identity': cazy_identity,
                                                    'cazy_align_length': cazy_align_length}})
        if ardb != None:
            with open(ardb, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        ardb_arg = line[head.index("ARG")]
                        ardb_identity = line[head.index("Identity(%)")]
                        ardb_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'ardb_arg': ardb_arg,
                                                    'ardb_identity': ardb_identity,
                                                    'ardb_align_length': ardb_align_length}})
        if card != None:
            with open(card, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        card_aro = line[head.index("ARO_accession")]
                        card_identity = line[head.index("Identity(%)")]
                        card_align_length = line[head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'card_aro': card_aro,
                                                    'card_identity': card_identity,
                                                    'card_align_length': card_align_length}})
        if vfdb != None:
            with open(vfdb, "rb") as f:
                head = f.next().strip().split("\t")
                for line in f:
                    line = line.strip().split('\t')
                    if not line[0] == "#Query":
                        gene = line[0]
                        vfdb_vfs = line[head.index("VFs")]
                        vfdb_identity = line[head.index("Identity(%)")]
                        vfdb_align_length = line[
                            head.index("Align_len")]
                        collection = self.db['anno_overview_detail']
                        collection.update({'query': gene}, {'$set': {'vfdb_vfs': vfdb_vfs,
                                                   'vfdb_identity': vfdb_identity,
                                                   'vfdb_align_length': vfdb_align_length}})
