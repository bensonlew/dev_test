# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import os
import re
import json
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


client = Config().mongo_client
db = client["sanger"]
LEVEL = {
    9: "OTU", 8: "s__", 7: "g__", 6: "f__", 5: "o__",
    4: "c__", 3: "p__", 2: "k__", 1: "d__"
}


def export_otu_table(otuId, targetPath):
    _try_write(targetPath)
    print "正在导出OTU表格:{}".format(targetPath)
    collection = db['sg_otu_specimen']
    my_collection = db['sg_specimen']
    results = collection.find({"otu_id": ObjectId(otuId)})
    samples = []
    for result in results:
        id_result = my_collection.find_one({"_id": result["specimen_id"]})
        if not id_result:
            raise Exception("意外错误，样本id:{}在sg_specimen中未找到！")
        samples.append(id_result["specimen_name"])
    collection = db['sg_otu_detail']
    with open(targetPath, "wb") as f:
        f.write("OTU ID\t%s\ttaxonomy\n" % "\t".join(samples))
        for col in collection.find({"otu_id": ObjectId(otuId)}):
            line = "%s\t" % col["otu"]
            for s in samples:
                line += "%s\t" % col[s]
            for cls in ["d__", "k__", "p__", "c__", "o__", "f__", "g__"]:
                if cls in col.keys():
                    line += "%s; " % col[cls]
            line += col["s__"]
            f.write("%s\n" % line)
    return targetPath


def export_otu_table_by_level(otuId, targetPath, level=9):
    _try_write(targetPath)
    level = int(level)
    print "正在导出级别为{}:{}的OTU表格:{}".format(level, LEVEL[level], targetPath)
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(otuId)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_specimen表中未找到！".format(otuId))
    samples = list()
    for result in results:
        if "specimen_id" not in result:
            raise Exception("otu_id:{}错误，请使用新导入的OTU表的id".format(otuId))
        sp_id = result['specimen_id']
        my_collection = db['sg_specimen']
        my_result = my_collection.find_one({"_id": sp_id})
        if not my_result:
            raise Exception("意外错误，样本id:{}在sg_specimen表里未找到".format(sp_id))
        samples.append(my_result["specimen_name"])
    collection = db['sg_otu_detail']
    name_dic = dict()
    results = collection.find({"otu_id": ObjectId(otuId)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_detail表中未找到！".format(otuId))
    for col in results:
        tmp = level + 1
        new_classify_name = _create_classify_name(col, tmp)
        if new_classify_name not in name_dic:
            name_dic[new_classify_name] = dict()
            for sp in samples:
                name_dic[new_classify_name][sp] = int(col[sp])
        else:
            for sp in samples:
                name_dic[new_classify_name][sp] += int(col[sp])
    with open(targetPath, "wb") as f:
        f.write("OTU ID\t%s\n" % "\t".join(samples))
        for k in name_dic.iterkeys():
            line = k
            for s in samples:
                line += "\t" + str(name_dic[k][s])
            line += "\n"
            f.write(line)
    return targetPath


def _create_classify_name(col, tmp):
    for i in range(1, 10):
        if LEVEL[i] not in col:
            raise Exception("Mongo数据库中的taxonomy信息不完整")
    new_col = list()
    for i in range(1, tmp):
        new_col.append(col[LEVEL[i]])
    return "; ".join(new_col)


def _try_write(targetPath):
    try:
        with open(targetPath, "wb") as w:
            pass
    except Exception as e:
        raise Exception(e)
    return True

def export_group_table(otuId, targetPath):
    _try_write(targetPath)
    print "正在按分组id和组名导出分组表"

