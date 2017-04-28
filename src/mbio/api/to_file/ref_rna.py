# -*- coding: utf-8 -*-
# __author__ = 'shijin'
import os
from biocluster.config import Config
from bson.objectid import ObjectId
import types
import json
import re
from types import StringTypes


# client = Config().mongo_client
# db = client[Config().MONGODB + '_ref_rna']


def export_gene_list(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    gene_list_path = os.path.join(dir_path, "%s_gene.list" % option_name)
    bind_obj.logger.debug("正在导出基因集")
    collection = db['sg_geneset_detail']
    main_collection = db['sg_geneset']
    my_result = main_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，geneset_id:{}在sg_geneset中未找到！".format(ObjectId(data)))
    results = collection.find({"geneset_id": ObjectId(data)})
    with open(gene_list_path, "wb") as f:
        for result in results:
            gene_id = result['gene_name']
            f.write(gene_id + "\n")
    return gene_list_path


def export_blast_table(data, option_name, dir_path, bind_obj=None):
    """
    获取blast结果
    """
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    nr_table_path = os.path.join(dir_path, "nr_{}.xls".format(option_name))
    gene_nr_table_path = os.path.join(dir_path, "gene_nr_{}.xls".format(option_name))
    sw_table_path = os.path.join(dir_path, "swissprot_{}.xls".format(option_name))
    gene_sw_table_path = os.path.join(dir_path, "gene_swissprot_{}.xls".format(option_name))
    blast_collection = db["sg_annotation_blast"]
    blast_result = blast_collection.find({"stat_id": ObjectId(data)})
    if not blast_result.count():
        raise Exception("stat_id:{}在sg_annotation_blast表中未找到".format(data))
    for result in blast_result:
        blast_id = result["_id"]
    collection = db["sg_annotation_blast_detail"]
    results = collection.find({"blast_id": blast_id})
    with open(nr_table_path, "wb") as w1, open(gene_nr_table_path, "wb") as w2, open(sw_table_path, "wb") as w3, open(gene_sw_table_path, "wb") as w4:
        header = "Score\tE-Value\tHSP-Len\tIdentity-%\tSimilarity-%\tQuery-Name\tQ-Len\tQ-Begin\t"
        header += "Q-End\tQ-Frame\tHit-Name\tHit-Len\tHsp-Begin\tHsp-End\tHsp-Frame\tHit-Description\n"
        w1.write(header)
        w2.write(header)
        w3.write(header)
        w4.write(header)
        for result in results:
            db = result["database"]
            anno_type = result["anno_type"]
            seq_type = result["seq_type"]
            score = result["score"]
            evalue = result["e_value"]
            hsp_len = result["hsp_len"]
            identity = result["identity_rate"]
            similarity = result["similarity_rate"]
            query_id = result["query_id"]
            hit_name = result["hit_name"]
            description = result["description"]
            q_len = result["q_len"]
            q_begin = result["q_begin"]
            q_end = result["q_end"]
            q_frame = result["q_frame"]
            hit_len = result["hit_len"]
            hsp_begin = result["hsp_begin"]
            hsp_end = result["hsp_end"]
            hsp_frame = result["hsp_frame"]
            line = str(score) + "\t" + str(evalue) + "\t" + str(hsp_len) + "\t" + str(identity) + "\t"
            line += str(similarity) + "\t" + query_id + "\t" + q_len + "\t" + q_begin + "\t" + q_end + "\t" + q_frame + "\t"
            line += hit_name + "\t"+ hit_len + "\t" + hsp_begin + "\t" + hsp_end + "\t" + hsp_frame + "\t" + description + "\n"
            if seq_type == "new":
                if db == "nr":
                    if anno_type == "transcript":
                        w1.write(line)
                    if anno_type == "gene":
                        w2.write(line)
                if db == "swissprot":
                    if anno_type == "transcript":
                        w3.write(line)
                    if anno_type == "gene":
                        w4.write(line)
    paths = ",".join([nr_table_path, gene_nr_table_path, sw_table_path, gene_sw_table_path])
    return paths


def export_go_list(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    go_list = os.path.join(dir_path, "GO.list")
    bind_obj.logger.debug("正在导出%sgo列表:%s" % (option_name, go_list))
    geneset_collection = db["sg_geneset"]
    task_id = geneset_collection.find_one({"_id": ObjectId(data)})["task_id"]
    my_result = db["sg_annotation_go"].find_one({"task_id": task_id})
    go_id = my_result["_id"]
    if not my_result:
        raise Exception("意外错误，annotation_go_id:{}在sg_annotation_go中未找到！".format(go_id))
    collection = db["sg_annotation_go_list"]
    results = collection.find({"go_id": ObjectId(go_id)})
    if not results:
        raise Exception("生成gos_list出错：annotation_id:{}在sg_annotation_gos_list中未找到！".format(ObjectId(go_id)))
    with open(go_list, "wb") as w:
        for result in results:
            gene_id = result["gene_id"]
            go_list = result["gos_list"]
            w.write(gene_id + "\t" + go_list + "\n")
    return go_list


def export_kegg_table(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    kegg_path = os.path.join(dir_path, 'gene_kegg_table.xls')
    bind_obj.logger.debug("正在导出参数%s的kegg_table文件，路径:%s" % (option_name, kegg_path))
    geneset_collection = db["sg_geneset"]
    geneset_result = geneset_collection.find_one({"_id": ObjectId(data)})
    task_id = geneset_result["task_id"]
    geneset_type = geneset_result["type"]
    my_result = db["sg_annotation_kegg"].find_one({"task_id": task_id})
    kegg_id = my_result["_id"]
    if not my_result:
        raise Exception("意外错误，annotation_kegg_id:{}在sg_annotation_kegg中未找到！".format(kegg_id))
    with open(kegg_path, 'wb') as w:
        w.write('#Query\tKO_ID(Gene id)\tKO_name(Gene name)\tHyperlink\tPaths\n')
        results = db['sg_annotation_kegg_table'].find({'$and': [{'kegg_id': kegg_id}, {'type': geneset_type}]})
        if not results:
            raise Exception("生成kegg_table出错：kegg_id:{}在sg_annotation_kegg_table中未找到！".format(ObjectId(kegg_id)))
        for result in results:
            w.write('{}\t{}\t{}\t{}\t{}\n'.format(result['query_id'], result['ko_id'], result['ko_name'], result['hyperlink'], result['paths']))
    return kegg_path


def export_all_list(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    all_list = os.path.join(dir_path, "all_gene.list")
    bind_obj.logger.debug("正在导出所有基因")
    collection = db['sg_geneset_detail']
    main_collection = db['sg_geneset']
    my_result = main_collection.find_one({'task_id': data, "type": "background"})
    print my_result["_id"]
    if not my_result:
        raise Exception("意外错误，task_id:{}的背景基因在sg_geneset中未找到！".format(data))
    results = collection.find({"geneset_id": ObjectId(my_result["_id"])})
    with open(all_list, "wb") as f:
        for result in results:
            gene_id = result['gene_name']
            f.write(gene_id + "\n")
    return all_list


def export_diff_express(data, option_name, dir_path, bind_obj=None):  # 需要修改
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
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


def export_cog_class(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    cog_path = os.path.join(dir_path, 'cog_class_table.xls')
    bind_obj.logger.debug("正在导出")
    genesets, table_title, task_id, geneset_type = get_geneset_detail(data)
    cog_collection = db["sg_annotation_cog"]
    cog_detail_collection = db["sg_annotation_cog_detail"]
    cog_id = cog_collection.find_one({"task_id": task_id})["_id"]
    cog_results = cog_detail_collection.find({'cog_id': cog_id})
    print table_title
    with open(cog_path, "wb") as w:
        w.write("Type\tFunctional Categoris\t" + "\t".join(table_title) + "\n")
        for cr in cog_results:
            kog_list = set(cr["kog_list"].split(";") if cr["kog_list"] else [])
            nog_list = set(cr["nog_list"].split(";") if cr["kog_list"] else [])
            cog_list = set(cr["cog_list"].split(";") if cr["kog_list"] else [])
            # print kog_list
            # write_line_key = cr["type"] + "\t" + cr["function_categories"]
            write_line = {}
            for gt in genesets:
                kog_count = list(kog_list & genesets[gt][1])
                nog_count = list(nog_list & genesets[gt][1])
                cog_count = list(cog_list & genesets[gt][1])
                if not len(kog_count) + len(nog_count) + len(cog_count) == 0:
                    write_line[gt] = [str(len(cog_count)), str(len(nog_count)), str(len(kog_count))]
            if len(write_line) > 0:
                w.write("{}\t{}\t".format(cr["type"], cr["function_categories"]))
                for tt in table_title:
                    w.write("\t".join(write_line[tt]) + "\t") if tt in write_line else w.write("0\t0\t0\t")
                # print write_line
                w.write("\n")
    return cog_path


def get_geneset_detail(data):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    geneset_collection = db["sg_geneset"]
    genesets = {}
    table_title = []
    print data.split(",")
    task_id = ""
    geneset_type = "gene"
    for geneset_id in data.split(","):
        # geneset_id = ObjectId(geneset_id)
        geneset_result = geneset_collection.find_one({"_id": ObjectId(geneset_id)})
        if not geneset_result:
            raise Exception("意外错误:未找到基因集_id为{}的基因集信息".format(geneset_id))
        task_id = geneset_result["task_id"]
        geneset_type = geneset_result["type"]
        geneset_name = geneset_result["name"]
        table_title.append(geneset_name)
        genesets[geneset_name] = [geneset_type]
        geneset_names = set()
        collection = db['sg_geneset_detail']
        results = collection.find({"geneset_id": ObjectId(geneset_id)})
        for result in results:
            gene_id = result['gene_name']
            geneset_names.add(gene_id)
        genesets[geneset_name].append(geneset_names)
    return genesets, table_title, task_id, geneset_type


def export_go_class(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    go_path = os.path.join(dir_path, 'go_class_table.xls')
    print("正在导出")
    genesets, table_title, task_id, seq_type = get_geneset_detail(data)
    print seq_type
    go_collection = db["sg_annotation_go"]
    go_level_collection = db["sg_annotation_go_level"]
    go_id = go_collection.find_one({"task_id": task_id})["_id"]
    go_results = go_level_collection.find({'go_id': go_id, "level": 2})
    # go_results = go_level_collection.find({'go_id': go_id, "level": 2, "anno_type": seq_type})
    # print table_title
    new_table_title = []
    for tt in table_title:
        new_table_title.append(tt + " num")
        new_table_title.append(tt + " percent")
    print new_table_title
    with open(go_path, "wb") as w:
        w.write("Term type\tTerm\tGO\t" + "\t".join(new_table_title) + "\n")
        for gr in go_results:
            seq_list = set(gr["seq_list"].split(";"))
            write_line = {}
            for gt in genesets:
                go_count = list(seq_list & genesets[gt][1])
                # print len(go_count)
                if not len(go_count) == 0:
                    write_line[gt] = str(len(go_count))
            if len(write_line):
                w.write("{}\t{}\t{}\t".format(gr["parent_name"], gr["term_type"], gr["go"]))
                for tt in table_title:
                    print tt
                    w.write(write_line[tt] + "\t") if tt in write_line else w.write("0\t")
                # print write_line
                # w.write("\t".join(write_line[write_line_key]))
                w.write("\n")
    return go_path


def export_gene_list_ppi(data, option_name, dir_path, bind_obj=None):
    db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
    gene_list_path = os.path.join(dir_path, "%s_list.txt" % option_name)
    bind_obj.logger.debug("正在导出基因集")
    collection = db['sg_geneset_detail']
    main_collection = db['sg_geneset']
    my_result = main_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，geneset_id:{}在sg_geneset中未找到！".format(ObjectId(data)))
    results = collection.find({"geneset_id": ObjectId(data)})
    with open(gene_list_path, "wb") as f:
        f.write("gene_id" + "\n")
        for result in results:
            gene_id = result['gene_name']
            f.write(gene_id + "\n")
    return gene_list_path
