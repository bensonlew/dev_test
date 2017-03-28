# -*- coding: utf-8 -*-
# __author__ = 'shijin'
import os
from biocluster.config import Config
from bson.objectid import ObjectId
import types
import json
import re
from types import StringTypes


client = Config().mongo_client
db = client[Config().MONGODB + '_ref_rna']

def export_kegg_table(data, option_name, dir_path, bind_obj=None):
    geneset_id = data
    col = db["sg_geneset"]
    result = col.find_one({"_id":ObjectId(geneset_id)})
    type = result["type"]
    kegg_path = os.path.join(dir_path, 'gene_kegg_table.xls')
    bind_obj.logger.debug("正在导出参数%s的kegg_table文件，路径:%s" % (option_name, kegg_path))
    with open(kegg_path, 'wb') as w:
        w.write('#Query\tKO_ID(Gene id)\tKO_name(Gene name)\tHyperlink\tPaths\n')
        task_id = bind_obj.sheet.id
        anno_id = db['sg_annotation'].find_one({'task_id': task_id})
        results = db['sg_annotation_kegg_table'].find({'$and': [{'annotation_id': anno_id}, {'type': type}]})
        if not results:
            raise Exception("生成kegg_table出错：annotation_id:{}在sg_annotation_kegg_table中未找到！".format(ObjectId(anno_id)))
        for result in results:
            w.write('{}\t{}\t{}\t{}\t{}\n'.format(result['query_id'], result['ko_id'], result['ko_name'],
                                                  result['hyperlink'], result['paths']))
    return kegg_path

def export_all_gene_list(data, option_name, dir_path, bind_obj=None):  # 需要修改
    all_list = os.path.join(dir_path, "all_gene.list")
    bind_obj.logger.debug("正在导出所有基因列表:%s" % all_list)
    par_collection = db["sg_denovo_express_diff"]
    express_id = par_collection.find_one({"_id": ObjectId(data)})["express_id"]
    collection = db["sg_denovo_express_detail"]
    my_collection = db["sg_denovo_express"]
    results = collection.find({"$and": [{"express_id": ObjectId(express_id)}, {"type": "gene"}]})
    my_result = my_collection.find_one({"_id": ObjectId(express_id)})
    if not my_result:
        raise Exception("意外错误，expree_id:{}在sg_denovo_express中未找到!".format(ObjectId(express_id)))
    with open(all_list, "wb") as w:
        for result in results:
            w.write(result["gene_id"] + "\n")
    return all_list

def export_diff_express(data, option_name, dir_path, bind_obj=None):  # 需要修改
    name = bind_obj.sheet.option("name")
    compare_name = bind_obj.sheet.option("compare_name")
    diff_express = os.path.join(dir_path, "%s_vs_%s.diff.exp.xls" % (name, compare_name))
    bind_obj.logger.debug("正在导出差异基因表达量表:%s" % diff_express)
    collection = db["sg_denovo_express_diff_detail"]
    results = collection.find({"$and": [{"express_diff_id": ObjectId(data)}, {"name": name}, {"compare_name": compare_name}]})
    my_collection = db["sg_denovo_express_diff"]
    my_result = my_collection.find_one({"_id": ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，expree_diff_id:{}在sg_denovo_express_diff中未找到!".format(ObjectId(data)))
    with open(diff_express, "wb") as w:
        w.write('gene_id\t{}_count\t{}_count\t{}_fpkm\t{}_fpkm\tlog2fc({}/{})\tpvalue\tfdr\tsignificant\tregulate\n'.format(name, compare_name, name, compare_name, compare_name, name))
        for result in results:
            gene_id = result["gene_id"]
            try:
                name_count = result["{}_count".format(name)]
                compare_count = result["{}_count".format(compare_name)]
                name_fpkm = result["{}_fpkm".format(name)]
                compare_fpkm = result["{}_fpkm".format(compare_name)]
            except:
                name_count = result["{}_mean_count".format(name)]
                compare_count = result["{}_mean_count".format(compare_name)]
                name_fpkm = result["{}_mean_fpkm".format(name)]
                compare_fpkm = result["{}_mean_fpkm".format(compare_name)]
            try:
                significant = result['significant']
                regulate = result['regulate']
                log = result["log2fc({}/{})".format(compare_name, name)]
                pval = result["pvalue"]
                fdr = result["fdr"]
            except:
                significant = result['Significant']
                regulate = result['Regulate']
                log = result["log2FC({}/{})".format(compare_name, name)]
                pval = result["Pvalue"]
                fdr = result["Fdr"]
            w.write(gene_id + '\t' + str(name_count) + '\t' + str(compare_count) + '\t' + str(name_fpkm) + '\t' + str(compare_fpkm) + '\t' + str(log) + '\t' + str(pval) + '\t' + str(fdr) +
'\t' + significant + '\t' + regulate + '\n')
    return diff_express