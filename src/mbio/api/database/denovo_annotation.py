# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import pymongo
import os
import re
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
import bson.binary
from cStringIO import StringIO

db = pymongo.MongoClient(host="192.168.10.189", port=27017).test_denovo_rna


def add_annotation(name, params):  
    project_sn = 'test_project'
    task_id = 'test_task'   
    insert_data = {
        'project_sn': project_sn,
        'task_id': task_id,
        'name': name if name else 'annotation test',
        'params': params,
        'status': 'end',
        'desc': '注释概况主表',
        'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
    } 
    collection = db['sg_denovo_annotation']
    annotation_id = collection.insert_one(insert_data).inserted_id 
    print "add sg_denovo_annotation sucess!"
    return annotation_id   

def add_annotation_stat_detail(task_id, stat_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！') 
    if not os.path.exists(stat_path):
        raise Exception('stat_path所指定的路径不存在，请检查！')
    data_list = []
    with open(stat_path, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('type', line[0]),
                ('transcript', line[1]),
                ('gene', line[2]),
                ('transcript_percent', line[3]),
                ('gene_percent', line[4]),
            ]
            data = SON(data)
            data_list.append(data) 
    try:
        collection = db['sg_denovo_annotation_stat_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "add sg_denovo_annotation_stat_detail failure"
    else:
        print "add sg_denovo_annotation_stat_detail sucess" 

def add_annotation_nr_detail(task_id, level_id, ncbi_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！') 
    if level_id:
        if level_id == 1:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_d.xls")
        elif level_id == 2: 
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_k.xls")
        elif level_id == 3:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_p.xls")
        elif level_id == 4:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_c.xls")
        elif level_id == 5:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_o.xls")
        elif level_id == 6:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_f.xls")
        elif level_id == 7:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_g.xls")
        elif level_id == 8:
            taxon_path = os.path.join(ncbi_path, "nr_taxon_stat_s.xls")
        else:
            raise Exception('分类水平超出范围，请检查！')
    else:
        raise Exception('分类水平未指定，请检查！') 
    data_list = []        
    with open(taxon_path, "r") as f:
        lines = f.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            data = [
                ('task_id', task_id),
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
        collection = db['sg_denovo_annotation_nr_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "add sg_denovo_annotation_nr_detail failure"
    else:
        print "add sg_denovo_annotation_nr_detail sucess" 

def add_annotation_pie(task_id, output_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！')
    type = ['gene', 'transcript']
    evalue_path = output_path + '/blast_nr_statistics/nr_evalue.xls'
    similar_path = output_path + '/blast_nr_statistics/nr_similar.xls'
    gene_evalue_path = output_path + '/anno_stat/blast_nr_statistics/gene_nr_evalue.xls'
    gene_similar_path = output_path + '/anno_stat/blast_nr_statistics/gene_nr_similar.xls'
    evalue_list,similar_list = [], []
    gene_evalue_list, gene_similar_list = [], []
    data_list = []
    with open(evalue_path, "r") as f1, open(similar_path, "r") as f2, open(gene_evalue_path, "r") as f3, open(gene_similar_path, "r") as f4:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
        lines3 = f3.readlines()
        lines4 = f4.readlines()        
        for line1 in lines1[1:]:
            line1 = line1.strip().split('\t') 
            evalue = '{' + "key:" + line1[0] + ",value:" + line1[1] + '}'
            evalue_list.append(evalue) 
        for line2 in lines2[1:]:
            line2 = line2.strip().split('\t')
            similar = '{' + "key:" + line2[0] + ",value:" + line2[1] + '}'
            similar_list.append(similar) 
        for line3 in lines3[1:]:
            line3 = line3.strip().split('\t') 
            gene_evalue = '{' + "key:" + line3[0] + ",value:" + line3[1] + '}'
            gene_evalue_list.append(gene_evalue) 
        for line4 in lines4[1:]:
            line4 = line4.strip().split('\t')
            gene_similar = '{' + "key:" + line4[0] + ",value:" + line4[1] + '}'
            gene_similar_list.append(gene_similar)
    data = [
        ('task_id', task_id),
        ('type', 'transcript'),
        ('evalue', evalue_list),
        ('similar', similar_list),
    ]     
    data = SON(data)
    data_list.append(data)
    data = [
        ('task_id', task_id),
        ('type', 'gene'),
        ('evalue', gene_evalue_list),
        ('similar', gene_similar_list),
    ]
    data = SON(data)
    data_list.append(data)    
    try:
        collection = db['sg_denovo_annotation_pie']
        collection.insert_many(data_list)
    except Exception, e:
        print "add sg_denovo_annotation_pie failure"
    else:
        print "add sg_denovo_annotation_pie sucess" 

def add_annotation_go_detail(task_id, output_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！')
    go_path = output_path + '/go/go1234level_statistics.xls'
    gene_go_path = output_path + '/anno_stat/go_stat/gene_go1234level_statistics.xls'
    data_list = list()
    with open(go_path, 'r') as f, open(gene_go_path, 'r') as f1:
        lines = f.readlines()
        lines1 = f1.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            level2 = line[2] + '(' + line[1] + ')'
            level3 = line[4] + '(' + line[3] + ')'
            data = [
                ('task_id', task_id),
                ('type', 'transcript'),
                ('level1', line[0]),
                ('level2', level2),
                ('level3', level3),
                ('level4', line[5]),
                ('seq_number', line[6]), 
            ]
            data = SON(data)
            data_list.append(data)
        for line1 in lines1[1:]:
            line1 = line1.strip().split('\t')
            gene_level2 = line1[2] + '(' + line1[1] + ')'
            gene_level3 = line1[4] + '(' + line1[3] + ')'
            data = [
                ('task_id', task_id),
                ('type', 'gene'),
                ('level1', line1[0]),
                ('level2', gene_level2),
                ('level3', gene_level3),
                ('level4', line1[5]),
                ('seq_number', line1[6]), 
            ]
            data = SON(data)
            data_list.append(data)
    try:
        collection = db['sg_denovo_annotation_go_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_annotation_go_detail failure"
    else:
        print "sg_denovo_annotation_go_detail sucess"

def add_annotation_go_graph(task_id, level, output_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！')
    level_path = output_path + '/go/go' + str(level) + 'level.xls' 
    gene_level_path = output_path + '/anno_stat/go_stat/gene_go' + str(level) + 'level.xls'
    data_list = list()
    with open(level_path, 'r') as f, open(gene_level_path, 'r') as f1:
        lines = f.readlines()
        lines1 = f1.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('level', level),
                ('type', 'transcript'),
                ('go_name', line[1]),
                ('parent_name', line[0]),
                ('num', line[3]),
                ('rate', line[4]),  
            ]
            data = SON(data)
            data_list.append(data)
        for line1 in lines1[1:]:
            line1 = line1.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('level', level),
                ('type', 'gene'),
                ('go_name', line1[1]),
                ('parent_name', line1[0]),
                ('num', line1[3]),
                ('rate', line1[4]),  
            ]
            data = SON(data)
            data_list.append(data)
    try:
        collection = db['sg_denovo_annotation_go_graph']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_annotation_go_graph failure"
    else:
        print "sg_denovo_annotation_go_graph sucess"

def add_annotation_cog_detail(task_id, output_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(task_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！') 
 
    cog_path = output_path + '/cog/cog_summary.xls'
    gene_cog_path = output_path + '/anno_stat/cog_stat/gene_cog_summary.xls'
    data_list = list()
    with open(cog_path, 'r') as f, open(gene_cog_path, 'r') as f1:
        lines = f.readlines()
        lines1 = f1.readlines()
        for line in lines[2:]:
            line = line.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('type', 'transcript'),
                ('functional_categories', line[1]),
                ('cog', line[2]),
                ('nog', line[3]),
                ('parent_name', line[0]), 
            ]
            data = SON(data)
            data_list.append(data)
        for line1 in lines1[2:]:
            line1 = line1.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('type', 'gene'),
                ('functional_categories', line1[1]),
                ('cog', line1[2]),
                ('nog', line1[3]),
                ('parent_name', line1[0]), 
            ]
            data = SON(data)
            data_list.append(data)
    try:
        collection = db['sg_denovo_annotation_cog_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_annotation_cog_detail failure"
    else:
        print "sg_denovo_annotation_cog_detail sucess"
 
def add_annotation_kegg_detail(task_id, output_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(blast_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！')
    kegg_path = output_path + '/kegg/kegg_layer.xls'
    gene_kegg_path = output_path + '/anno_stat/kegg_stat/gene_kegg_layer.xls'
    data_list = list()
    with open(kegg_path, 'r') as f1, open(gene_kegg_path, 'r') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()
        i =1
        for i in range(len(lines1)):
            lines1[i] = lines1[i].strip().split('\t')
            lines2[i] = lines2[i].strip().split('\t')
            if lines1[i][0] == lines2[i][0]:
                if lines1[i][1] == lines2[i][1]:
                    data = [
                        ('task_id', task_id),
                        ('catergory', lines1[i][1]),
                        ('transcripts_num', lines1[i][2]),
                        ('genes_num', lines2[i][2]), 
                    ] 
                    data = SON(data)
                    data_list.append(data)
    try:
        collection = db['sg_denovo_annotation_kegg_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_annotation_kegg_detail failure"
    else:
        print "sg_denovo_annotation_go_detail sucess"
        
def add_annotation_query(task_id, query_path):
    if not isinstance(task_id, ObjectId):
        if isinstance(blast_id, types.StringTypes):
            task_id = ObjectId(task_id)
        else:
            raise Exception('task_id必须为ObjectId对象或其对应的字符串！')
    if not os.path.exists(query_path):
        raise Exception('query_path所指定的路径不存在，请检查！') 
    data_list = list()
    with open(query_path, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            data = [
                ('task_id', task_id),
                ('transcript', line[0]),
                ('gene', line[1]),
                ('nr_hit_name', line[2]),                
                ('nr_detail_id', ''),
                ('go_detail_id', ''),
                ('kegg_detail_id', ''),
                ('cog_detail_id', ''),
            ]
            try:
                data += [('nr_taxonomy', line[3]),]
            except:
                data += [('nr_taxonomy', ''),]
            try:
                data += [('kegg_pathway', line[8]),]
            except:
                data += [('kegg_pathway', ''),] 
            try:
                data += [('cog', line[4]),]
            except:
                data += [('cog', ''),]  
            try:
                data += [('go_id', line[5]),]
            except:
                data += [('go_id', ''),]   
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
        collection = db['sg_denovo_annotation_query']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_annotation_query failure"
    else:
        print "sg_denovo_annotation_query sucess"

def add_blast(name=None, blast_version=None, blast_pro=None, blast_db=None, e_value=None):
    project_sn = 'test_project'
    task_id = 'test_task'
    insert_data = {
        'project_sn': project_sn,
        'task_id': task_id,
        'name': name if name else 'annotation blast',
        'status': 'end',
        'desc': 'blast最佳比对结果主表',
        'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M%S'),
        'blast_version': blast_version,
        'blast_pro': blast_pro,
        'blast_db': blast_db,
        'e_value': e_value
    }
    collection = db['sg_denovo_blast']
    blast_id = collection.insert_one(insert_data).inserted_id 
    print "add sg_denovo_blast sucess!"
    return blast_id 
        
def add_blast_table_detail(blast_id, blast_db, output_path):
    if not isinstance(blast_id, ObjectId):
        if isinstance(blast_id, types.StringTypes):
            blast_id = ObjectId(blast_id)
        else:
            raise Exception('blast_id必须为ObjectId对象或其对应的字符串！') 
    blast_path = output_path + '/' + blast_db + 'blast/Trinity_vs_' + blast_db + '.xls'
    gene_blast_path = output_path + '/anno_stat/blast/' + 'gene_' + blast_db + '.xls'
    data_list = []
    with open(blast_path, 'r') as f, open(gene_blast_path, 'r') as f1:
        lines = f.readlines()
        lines1 = f1.readlines()
        for line in lines[1:]:
            line = line.strip().split('\t')
            data = [ 
                ('blast_id', blast_id), 
                ('type', 'transcript'),
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
            data = SON(data)
            data_list.append(data)
        for line1 in lines1[1:]:
            line1 = line1.strip().split('\t')
            data = [ 
                ('blast_id', blast_id), 
                ('type', 'gene'),
                ('score', line1[0]),
                ('e_value', line1[1]),
                ('hsp_len', line1[2]),
                ('identity_rate', line1[3]),
                ('similarity', line1[4]),
                ('query_name', line1[5]),
                ('q_len', line1[6]),
                ('q_begin', line1[7]),
                ('q_end', line1[8]),
                ('q_frame', line1[9]),
                ('hit_name', line1[10]),
                ('hit_len', line1[11]),
                ('hsp_begin', line1[12]),
                ('hsp_end', line1[13]),
                ('hsp_frame', line1[14]),
                ('hit_discription', line1[15]), 
            ]
            data = SON(data)
            data_list.append(data)
    try:
        collection = db['sg_denovo_blast_table_detail']
        collection.insert_many(data_list)
    except Exception, e:
        print "sg_denovo_blast_table_detail failure"
    else:
        print "sg_denovo_blast_table_detail sucess"
        
        

output_path = '/mnt/ilustre/users/sanger-dev/workspace/20161028/Single_denovo_anno/DenovoAnnotation/output'        
task_id = add_annotation('', '') 
add_annotation_stat_detail(task_id, '/mnt/ilustre/users/sanger-dev/workspace/20161028/Single_denovo_anno/DenovoAnnotation/output/anno_stat/all_annotation_statistics.xls')
add_annotation_nr_detail(task_id, 2, '/mnt/ilustre/users/sanger-dev/workspace/20161028/Single_denovo_anno/DenovoAnnotation/output/anno_stat/ncbi_taxonomy') 
add_annotation_pie(task_id, output_path)
add_annotation_go_detail(task_id, output_path)
add_annotation_go_graph(task_id, 2, output_path)
add_annotation_cog_detail(task_id, output_path)
add_annotation_kegg_detail(task_id, output_path)
add_annotation_query(task_id, '/mnt/ilustre/users/sanger-dev/workspace/20161028/Single_denovo_anno/DenovoAnnotation/output/anno_stat/all_annotation.xls')
blast_pro = 'blastx'
blast_db = 'kegg'
blast_id = add_blast('blast', '2.3.0', blast_pro, blast_db, 'le-5')
add_blast_table_detail(blast_id, blast_db, output_path)