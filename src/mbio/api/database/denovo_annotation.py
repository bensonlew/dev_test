# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import os
import re
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class DenovoAnnotation(Base):
    def __init__(self, bind_object):
        super(DenovoAnnotation, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_annotation(self, name=None, params=None, anno_stat_dir=None):
        """
        level_id:分类水平，为列表,范围：[1,2,3,4,5,6,7,8]，分别对应域界门纲目科属种
        level：go层级水平，为列表，范围：[2,3,4]
        """
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        # task_id = 'test_qiu'
        # project_sn = 'test_qiu'
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'annotation_stat_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': '注释概况主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        collection = self.db['sg_denovo_annotation']
        annotation_id = collection.insert_one(insert_data).inserted_id
        if anno_stat_dir:
            all_stat_path = anno_stat_dir + '/anno_stat/all_annotation_statistics.xls'
            self.add_annotation_stat_detail(annotation_id=annotation_id, stat_path=all_stat_path)
            taxon_path = anno_stat_dir + '/anno_stat/ncbi_taxonomy/'
            for i in os.listdir(taxon_path):
                if re.search(r'_d\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=1, taxon_path=taxon_path + i)
                if re.search(r'_k\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=2, taxon_path=taxon_path + i)
                if re.search(r'_p\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=3, taxon_path=taxon_path + i)
                if re.search(r'_c\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=4, taxon_path=taxon_path + i)
                if re.search(r'_o\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=5, taxon_path=taxon_path + i)
                if re.search(r'_f\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=6, taxon_path=taxon_path + i)
                if re.search(r'_g\.xls$', i):
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=7, taxon_path=taxon_path + i)
                if re.search(r'_s\.xls$', i):
                    # print taxon_path + i
                    self.add_annotation_nr_detail(annotation_id=annotation_id, level_id=8, taxon_path=taxon_path + i)
            gene_nr_pie = anno_stat_dir + '/anno_stat/blast_nr_statistics/'
            nr_pie = anno_stat_dir + '/blast_nr_statistics/'
            self.add_annotation_pie(annotation_id=annotation_id, evalue_path=gene_nr_pie + 'gene_nr_evalue.xls', similar_path=gene_nr_pie + 'gene_nr_similar.xls', query_type='gene')
            self.add_annotation_pie(annotation_id=annotation_id, evalue_path=nr_pie + 'nr_evalue.xls', similar_path=nr_pie + 'nr_similar.xls', query_type='transcript')
            self.add_annotation_go_detail(annotation_id=annotation_id, go_path=anno_stat_dir + '/go/go1234level_statistics.xls', query_type='transcript')
            self.add_annotation_go_detail(annotation_id=annotation_id, go_path=anno_stat_dir + '/anno_stat/go_stat/gene_go1234level_statistics.xls', query_type='gene')
            go_files = os.listdir(anno_stat_dir + '/anno_stat/go_stat/') + os.listdir(anno_stat_dir + '/go')
            for i in go_files:
                if re.search(r'^gene.*level\.xls$', i):
                    level = i.split('level.xls')[-1]
                    self.add_annotation_go_graph(annotation_id=annotation_id, level=level, level_path=anno_stat_dir + '/anno_stat/go_stat/' + i, query_type='gene')
                if re.search(r'^go.level\.xls$', i):
                    level = i.split('level.xls')[-1]
                    self.add_annotation_go_graph(annotation_id=annotation_id, level=level, level_path=anno_stat_dir + '/go/' + i, query_type='transcript')
            self.add_annotation_cog_detail(annotation_id=annotation_id, cog_path=anno_stat_dir + '/cog/cog_summary.xls', query_type='transcript')
            self.add_annotation_cog_detail(annotation_id=annotation_id, cog_path=anno_stat_dir + '/anno_stat/cog_stat/gene_cog_summary.xls', query_type='gene')
            self.add_annotation_kegg_detail(annotation_id=annotation_id, kegg_path=anno_stat_dir + '/kegg/kegg_layer.xls', gene_kegg_path=anno_stat_dir + '/anno_stat/kegg_stat/gene_kegg_layer.xls')
            self.add_annotation_query(annotation_id=annotation_id, query_path=anno_stat_dir + '/anno_stat/all_annotation.xls')
            venn_dir = anno_stat_dir + '/anno_stat/venn/'
            self.add_venn(venn_dir=venn_dir, anno_id=annotation_id)
        print "add sg_denovo_annotation sucess!"
        return annotation_id

    @report_check
    def add_annotation_stat_detail(self, annotation_id, stat_path):
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(stat_path))
        data_list = []
        with open(stat_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('annotation_id', annotation_id),
                    ('type', line[0]),
                    ('transcript', line[1]),
                    ('gene', line[2]),
                    ('transcript_percent', line[3]),
                    ('gene_percent', line[4]),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_stat_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "add sg_denovo_annotation_stat_detail failure{}".format(e)
        else:
            print "add sg_denovo_annotation_stat_detail sucess"

    @report_check
    def add_annotation_nr_detail(self, annotation_id, level_id, taxon_path):
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if level_id not in [1, 2, 3, 4, 5, 6, 7, 8]:
                raise Exception('分类水平超出范围，请检查！')
        if not os.path.exists(taxon_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(taxon_path))
        data_list = []
        with open(taxon_path, "r") as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('annotation_id', annotation_id),
                    ('level_id', level_id),
                    ('taxon', line[0]),
                    ('transcripts', line[1]),
                    ('genes', line[2]),
                    ('transcripts_percent', line[3]),
                    ('genes_percent', line[4]),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_nr_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "add sg_denovo_annotation_nr_detail failure{}".format(e)
        else:
            print "add sg_denovo_annotation_nr_detail sucess"

    @report_check
    def add_annotation_pie(self, annotation_id, evalue_path, similar_path, query_type=None):
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(evalue_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(evalue_path))
        if not os.path.exists(similar_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(similar_path))
        evalue_list, similar_list = [], []
        data_list = []
        with open(evalue_path, "r") as f1, open(similar_path, "r") as f2:
            lines1 = f1.readlines()
            lines2 = f2.readlines()
            for line1 in lines1[1:]:
                line1 = line1.strip().split('\t')
                evalue = {"key": line1[0], "value": line1[1]}
                evalue_list.append(evalue)
            for line2 in lines2[1:]:
                line2 = line2.strip().split('\t')
                similar = {"key": line2[0], "value": line2[1]}
                similar_list.append(similar)
        data = [
            ('annotation_id', annotation_id),
            ('evalue', evalue_list),
            ('similar', similar_list),
        ]
        if query_type:
            data.append(('type', query_type))
        data = SON(data)
        data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_pie']
            collection.insert_many(data_list)
        except Exception, e:
            print "add sg_denovo_annotation_pie failure{}".format(e)
        else:
            print "add sg_denovo_annotation_pie sucess"

    @report_check
    def add_annotation_go_detail(self, annotation_id, go_path, query_type=None):
        '''go_path:go1234level_statistics.xls'''
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(go_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(go_path))
        data_list = list()
        with open(go_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                level2 = line[2] + '(' + line[1] + ')'
                level3 = line[4] + '(' + line[3] + ')'
                data = [
                    ('annotation_id', annotation_id),
                    ('level1', line[0]),
                    ('level2', level2),
                    ('level3', level3),
                    ('level4', line[5]),
                    ('seq_number', line[6]),
                ]
                if query_type:
                    data.append(('type', query_type))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_go_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_annotation_go_detail failure{}".format(e)
        else:
            print "sg_denovo_annotation_go_detail sucess"

    @report_check
    def add_annotation_go_graph(self, annotation_id, level, level_path, query_type):
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(level_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(level_path))
        data_list = list()
        with open(level_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('annotation_id', annotation_id),
                    ('level', level),
                    ('go_name', line[1]),
                    ('parent_name', line[0]),
                    ('num', line[3]),
                    ('rate', line[4]),
                    ('go_id', line[2]),
                    ('sequence', line[5]),
                ]
                if query_type:
                    data.append(('type', query_type))
                data = SON(data)
                data_list.append(data) 
        try:
            collection = self.db['sg_denovo_annotation_go_graph']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_annotation_go_graph failure{}".format(e)
        else:
            print "sg_denovo_annotation_go_graph sucess"

    @report_check
    def add_annotation_cog_detail(self, annotation_id, cog_path, query_type=None):
        '''
        cog_path:cog_summary.xls
        '''
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(cog_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(cog_path))
        data_list = list()
        with open(cog_path, 'r') as f:
            lines = f.readlines()
            for line in lines[2:]:
                line = line.strip().split('\t')
                data = [
                    ('annotation_id', annotation_id),
                    ('functional_categories', line[1]),
                    ('cog', line[2]),
                    ('nog', line[3]),
                    ('parent_name', line[0]),
                ]
                if query_type:
                    data.append(('type', query_type))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_cog_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_annotation_cog_detail failure{}".format(e)
        else:
            print "sg_denovo_annotation_cog_detail sucess"

    @report_check
    def add_annotation_kegg_detail(self, annotation_id, kegg_path, gene_kegg_path):
        '''
        kegg_path:kegg_layer.xls
        gene_kegg_path:gene_kegg_layer.xls
        '''
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(kegg_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(kegg_path))
        if not os.path.exists(gene_kegg_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(gene_kegg_path))
        data_list = list()
        with open(kegg_path, 'r') as f1, open(gene_kegg_path, 'r') as f2:
            lines1 = f1.readlines()
            lines2 = f2.readlines()
            i = 1
            for i in range(len(lines1)):
                lines1[i] = lines1[i].strip().split('\t')
                lines2[i] = lines2[i].strip().split('\t')
                if lines1[i][0] == lines2[i][0]:
                    if lines1[i][1] == lines2[i][1]:
                        data = [
                            ('annotation_id', annotation_id),
                            ('catergory', lines1[i][1]),
                            ('transcripts_num', lines1[i][2]),
                            ('genes_num', lines2[i][2]),
                        ]
                        data = SON(data)
                        data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_kegg_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_annotation_kegg_detail failure{}".format(e)
        else:
            print "sg_denovo_annotation_go_detail sucess"

    @report_check
    def add_annotation_query(self, annotation_id, query_path):
        if not isinstance(annotation_id, ObjectId):
            if isinstance(annotation_id, types.StringTypes):
                annotation_id = ObjectId(annotation_id)
            else:
                raise Exception('annotation_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(query_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(query_path))
        data_list = list()
        with open(query_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('annotation_id', annotation_id),
                    ('transcript', line[0]),
                    ('gene', line[1]),
                    ('nr_hit_name', line[2]),
                    ('nr_detail_id', ''),
                    ('go_detail_id', ''),
                    ('kegg_detail_id', ''),
                    ('cog_detail_id', ''),
                ]
                try:
                    data += [('nr_taxonomy', line[3])]
                except:
                    data += [('nr_taxonomy', '')]
                try:
                    data += [('kegg_pathway', line[8])]
                except:
                    data += [('kegg_pathway', '')]
                try:
                    data += [('cog', line[4])]
                except:
                    data += [('cog', '')]
                try:
                    data += [('go_id', line[5])]
                except:
                    data += [('go_id', '')]
                try:
                    data += [
                        ('ko_id', line[6]),
                        ('ko_name', line[7]),
                    ]
                except:
                    data += [
                        ('ko_id', ''),
                        ('ko_name', ''),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_annotation_query']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_annotation_query failure{}".format(e)
        else:
            print "sg_denovo_annotation_query sucess"

    @report_check
    def add_blast(self, name=None, blast_version='2.3.0+', blast_pro=None, blast_db=None, e_value=None, blast_path=None, gene_list=None):
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'annotation blast' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'status': 'end',
            'desc': 'blast最佳比对结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M%S'),
            'blast_version': blast_version,
            'blast_pro': blast_pro,
            'blast_db': blast_db,
            'e_value': e_value
        }
        collection = self.db['sg_denovo_blast']
        blast_id = collection.insert_one(insert_data).inserted_id
        if blast_path:
            self.add_blast_table_detail(blast_id, blast_path, gene_list)
        print "add sg_denovo_blast sucess!"
        return blast_id

    @report_check
    def add_blast_table_detail(self, blast_id, blast_path, gene_list=None):
        """
        gene_list:基因序列列表，由trinity组装获得。
        """
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
            for line in lines[1:]:
                line = line.strip().split('\t')
                data = [
                    ('blast_id', blast_id),
                    ('score', line[0]),
                    ('e_value', line[1]),
                    ('hsp_len', line[2]),
                    ('identity_rate', line[3]),
                    ('similarity', line[4]),
                    ('query_name', line[5]),
                    ('q_len', line[6]),
                    ('q_begin', line[7]),
                    ('q_end', line[8]),
                    ('q_frame', line[9]),
                    ('hit_name', line[10]),
                    ('hit_len', line[11]),
                    ('hsp_begin', line[12]),
                    ('hsp_end', line[13]),
                    ('hsp_frame', line[14]),
                    ('hit_discription', line[15]),
                ]
                if gene_list:
                    if line[5] in gene_list:
                        data += [('type', 'gene'), ('gene_id', line[5].split('_i')[0])]
                    else:
                        data.append(('type', 'transcript'))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_denovo_blast_table_detail']
            collection.insert_many(data_list)
        except Exception, e:
            print "sg_denovo_blast_table_detail failure{}".format(e)
        else:
            print "sg_denovo_blast_table_detail sucess"

    def add_venn(self, venn_dir, anno_id):
        files = os.listdir(venn_dir)
        for f in files:
            data = {'annotation_id': ObjectId(anno_id)}
            path = venn_dir + '/' + f
            with open(path, 'rb') as r:
                venn_list = r.readline().strip('\n')
                for line in r:
                    venn_list += ',{}'.format(line.strip('\n'))
                data['venn_list'] = venn_list
            if f == 'nr_venn.txt':
                data['db_name'] = 'nr'
                data['type'] = 'transcript'
            elif f == 'string_venn.txt':
                data['db_name'] = 'string'
                data['type'] = 'transcript'
            elif f == 'kegg_venn.txt':
                data['db_name'] = 'kegg'
                data['type'] = 'transcript'
            elif f == 'gene_nr_venn.txt':
                data['db_name'] = 'nr'
                data['type'] = 'gene'
            elif f == 'gene_string_venn.txt':
                data['db_name'] = 'string'
                data['type'] = 'gene'
            else:
                data['db_name'] = 'kegg'
                data['type'] = 'gene'
            collection = self.db['sg_denovo_annotation_venn']
            collection.insert_one(data)

if __name__ == '__main__':  # for test
    a = DenovoAnnotation(bind_object='aaaa')
    a.add_annotation(name=None, params=None, anno_stat_dir='/mnt/ilustre/users/sanger-dev/sg-users/qiuping/denovo/tool_test/se_test/files/anno_result/DenovoAnnotation/output/')
    # a.add_venn(venn_dir='/mnt/ilustre/users/sanger-dev/workspace/20161107/Single_denovo_anno_stat/DenovoAnnoStat/output/venn', anno_id='582012c4a4e1af76414b6b03')
