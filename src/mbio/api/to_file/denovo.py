# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import os
from biocluster.config import Config
from bson.objectid import ObjectId
import types
import json
import re
from types import StringTypes


client = Config().mongo_client
db = client[Config().MONGODB + '_rna']


def export_express_matrix(data, option_name, dir_path, bind_obj=None):
    fpkm_path = os.path.join(dir_path, "%s_fpkm.matrix" % option_name)
    count_path = os.path.join(dir_path, "%s_count.matrix" % option_name)
    bind_obj.logger.debug("正在导出计数矩阵:%s；fpkm矩阵:%s" % (count_path, fpkm_path))
    collection = db['sg_denovo_express_detail']
    my_collection = db['sg_denovo_express']
    results = collection.find({'$and': [{'express_id': ObjectId(data)}, {'type': 'gene'}]})
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，express_id:{}在sg_denovo_express中未找到！".format(ObjectId(data)))
    samples = my_result['specimen']
    with open(fpkm_path, "wb") as f, open(count_path, 'wb') as c:
        head = '\t'.join(samples)
        f.write('\t' + head + '\n')
        c.write('\t' + head + '\n')
        for result in results:
            gene_id = result['gene_id']
            fpkm_write = '{}'.format(gene_id)
            count_write = '{}'.format(gene_id)
            for sam in samples:
                fpkm = sam + '_fpkm'
                count = sam + '_count'
                fpkm_write += '\t{}'.format(result[fpkm])
                count_write += '\t{}'.format(result[count])
            fpkm_write += '\n'
            count_write += '\n'
            f.write(fpkm_write)
            c.write(count_write)
    paths = ','.join([fpkm_path, count_path])
    return paths


def export_control_file(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, '{}.txt'.format(option_name))
    bind_obj.logger.debug("正在导出计数矩阵:%s" % file_path)
    collection = db['sg_denovo_control']
    result = collection.find_one({'_id': ObjectId(data)})
    if not result:
        raise Exception("意外错误，control_id:{}在sg_denovo_control中未找到！".format(ObjectId(data)))
    group_id = result['group_id']
    if group_id not in ['all', 'All', 'ALL']:
        if isinstance(group_id, types.StringTypes):
            group_id = ObjectId(group_id)
        group_coll = db['sg_denovo_specimen_group']
        g_result = group_coll.find_one({'_id': group_id})
        if not g_result:
            raise Exception("意外错误，control_file的group_id:{}在sg_denovo_specimen_group中未找到！".format(group_id))
    control_detail = result['control_names']
    with open(file_path, 'wb') as w:
        w.write('#control\t{}\n'.format(result['scheme_name']))
        for i in control_detail:
            w.write('{}\t{}\n'.format(i.keys()[0], i.values()[0]))
    return file_path


def export_group_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按分组的详细信息获取group表
    使用时确保你的workflow的option里group_detal这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    if data in ["all", "All", "ALL"]:
        with open(file_path, "wb") as f:
            f.write("#sample\t" + "##empty_group##" + "\n")
        return file_path
    data = _get_objectid(data)
    group_detail = bind_obj.sheet.option('group_detail')
    group_table = db['sg_denovo_specimen_group']
    if not isinstance(group_detail, dict):
        try:
            table_dict = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_denovo_specimen_group表里找到相应的记录".format(data))
    schema_name = re.sub("\s", "_", group_schema["group_name"])  # 将分组方案名的空格替换成下划线
    with open(file_path, "wb") as f:
        f.write("#sample\t" + schema_name + "\n")
    sample_table_name = 'sg_denovo_specimen'
    sample_table = db[sample_table_name]
    with open(file_path, "ab") as f:
        for k in table_dict:
            for sp_id in table_dict[k]:
                sp = sample_table.find_one({"_id": ObjectId(sp_id)})
                if not sp:
                    raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(sp_id, sample_table_name))
                else:
                    sp_name = sp["specimen_name"]
                f.write("{}\t{}\n".format(sp_name, k))
    return file_path


def _get_objectid(data):
    if not isinstance(data, ObjectId):
        if not isinstance(data, StringTypes):
            raise Exception("{}不为ObjectId类型或者其对应的字符串".format(data))
        else:
            try:
                data = ObjectId(data)
            except:
                raise Exception("{}不为ObjectId类型或者其对应的字符串".format(data))
    return data


def export_bam_path(data, option_name, dir_path, bind_obj=None):
    my_collection = db['sg_denovo_express']
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，express_id:{}在sg_denovo_express中未找到！".format(ObjectId(data)))
    bam_dir = my_result['bam_path']
    dir_path = bam_dir
    return dir_path


def export_bed_path(data, option_name, dir_path, bind_obj=None):
    my_collection = db['sg_denovo_orf']
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，id:{}在sg_denovo_orf中未找到！".format(ObjectId(data)))
    bam_dir = my_result['orf-bed']
    dir_path = bam_dir
    return dir_path


def export_fasta_path(data, option_name, dir_path, bind_obj=None):
    my_collection = db['sg_denovo_sequence']
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，id:{}在sg_denovo_sequence中未找到！".format(ObjectId(data)))
    gene_path = my_result['gene_path']
    dir_path = gene_path
    return dir_path

def go_enrich(data, option_name, dir_path, bind_obj=None):
    all_list = os.path.join(dir_path, "all_gene.list")
    diff_list = os.path.join(dir_path, "unigene.list")
    gos_list = os.path.join(dir_path, "unigene_gos.list")
    collection = db["sg_denovo_express_detail"]
    my_collection = db["sg_denovo_express"]
    results = collection.find({"$and": [{"express_id": ObjectId(data)}, {"type": "gene"}]})
    my_result = my_collection.find_one({"_id": ObjectId(data)})
    task_id = my_result["task_id"]
    if not my_result:
        raise Exception("意外错误，expree_id:{}在sg_denovo_express中未找到!".format(ObjectId(data)))
    with open(all_list, "wb") as w1, open(diff_list, "wb") as w2:
        for result in results:
            gene_id = result["gene_id"]
            w1.write(gene_id + "\n")
        collection1 = db["sg_denovo_express_diff"]
        results1 = collection1.find({"express_id": ObjectId(data)})
        #results1 = collection1.find_one({"express_id": data})
        for result1 in results1:
            express_diff_id = result1["_id"]
            collection2 = db["sg_denovo_express_diff_detail"]
            results2 = collection2.find({"express_diff_id": express_diff_id})
            for result2 in results2:
                gene_id = result2["gene_id"]
                w2.write(gene_id + "\n")
    my_collection1 = db["sg_denovo_annotation"]
    my_result1 = my_collection1.find_one({"task_id": task_id})
    annotation_id = my_result1["_id"]
    if not my_result1:
        raise Exception("意外错误，annotation_id:{}在sg_denovo_annotation中未找到！".format(annotation_id))
    collection3 = db["sg_denovo_annotation_gos_list"]
    results3 = collection3.find({"annotation_id": ObjectId(annotation_id)})
    with open(gos_list, "wb") as w4:
        for result3 in results3:
            gene_id = result3["gene_id"]
            go_list = result3["go_id"]
            w4.write(gene_id + "\t" + go_list + "\n")
    paths = ','.join([all_list, diff_list, gos_list])
    return paths

def go_regulate(data, option_name, dir_path, bind_obj=None):
    diff_express = os.path.join(dir_path, "diff.exp.xls")
    go2level = os.path.join(dir_path, "go2level.xls")
    my_collection = db["sg_denovo_express"]
    my_result = my_collection.find_one({"_id": ObjectId(data)})
    task_id = my_result["task_id"]
    if not my_result:
        raise Exception("意外错误，expree_id:{}在sg_denovo_express中未找到!".format(ObjectId(data)))
    with open(diff_express, 'wb') as w:
        w.write('seq_id\tE20_1_count\tE20_2_count\tP1_1_count\tP1_2_count\tE20_1_fpkm\tE20_2_fpkm\tP1_1_fpkm\tP1_2_fpkm\tE20_mean_fpkm\tP1_mean_fpkm\tlogFC(P1/E20)\tPvalue\tFDR\tSignificant\tRegulate\n')
        collection1 = db["sg_denovo_express_diff"]
        #results1 = collection1.find({"express_id": ObjectId(data)})
        results1 = collection1.find({"express_id": ObjectId("583ceba958e7b77d4c60d984")})
        for result1 in results1:
            express_diff_id = result1["_id"]
            collection2 = db["sg_denovo_express_diff_detail"]
            results2 = collection2.find({"express_diff_id": express_diff_id})
            for result2 in results2:
                gene_id = result2["gene_id"]
                significant = result2['significant']  #
                regulate = result2['regulate'] #
                w.write(gene_id + '\t''\t''\t''\t''\t''\t''\t''\t''\t''\t''\t''\t''\t''\t' + significant + '\t' + regulate + '\n')
    my_collection1 = db["sg_denovo_annotation"]
    my_result1 = my_collection1.find_one({"task_id": task_id})
    annotation_id = my_result1["_id"]
    if not my_result1:
        raise Exception("意外错误，annotation_id:{}在sg_denovo_annotation中未找到！".format(annotation_id))
    collection3 = db["sg_denovo_annotation_go_graph"]
    results3 = collection3.find({"$and": [{"annotation_id": annotation_id}, {"type": "gene"}, {"level": 2}]})
    with open(go2level, "wb") as w:
        w.write("term_type\tterm\tGO\tnumber\tpercent\tsequence\n")
        for result3 in results3:
            term_type = result3['parent_name']
            term = result3['go_name']
            GO = result3['go_id']
            number = result3['num']
            percent = result3['rate']
            sequence = result3['sequence']
            w.write(term_type + '\t' + term + '\t' + GO + '\t' + number + '\t' + percent + '\t' + sequence + '\n')
    paths = ','.join([diff_express, go2level])
    return paths

def go_enrich_annotation(data, option_name, dir_path, bind_obj=None):
    gos_list = os.path.join(dir_path, "unigene_gos.list")
    collection = db["sg_denovo_annotation_gos_list"]
    results = collection.find({"annotation_id": ObjectId(data)})
    my_collection = db["sg_denovo_annotation"]
    my_result = my_collection.find_one({"_id": ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，annotation_id:{}在sg_denovo_annotation中未找到！".format(ObjectId(data)))
    with open(gos_list, "wb") as w:
        for result in results:
            gene_id = result["gene_id"]
            go_list = result["go_id"]
            w.write(gene_id + "\t" + go_list + "\n")
    return gos_list
