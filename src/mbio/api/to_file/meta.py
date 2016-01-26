# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
from collections import defaultdict
from pymongo import MongoClient
from biocluster.config import Config
from bson.objectid import ObjectId


client = MongoClient(Config().MONGO_URI)
db = client["sanger"]


def export_otu_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.otu.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    samples = []
    for result in results:
        samples.append(result["specimen_name"])
    # samples = result["specimen_names"]
    collection = db['sg_otu_detail']
    with open(file_path, "wb") as f:
        f.write("OTU ID\t%s\ttaxonomy\n" % "\t".join(samples))
        for col in collection.find({"_id": ObjectId(data)}):
            line = "%s" % col["otu"]
            for s in samples:
                line += "\t%s" % col[s]
            for cls in ["d__", "k__", "p__", "c__", "o__", "f__", "g__"]:
                if cls in col.keys():
                    line += "\t%s; " % col[cls]
            f.write("%s\n" % line)
    return file_path


def export_otu_table_by_level(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.otu.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_otu']
    result = collection.find_one({"_id": ObjectId(data)})
    samples = result["specimen_names"]
    level = bind_obj.sheet.option("level")
    LEVEL = {
        1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
        6: "f__", 7: "g__", 8: "s__", 9: "otu"
    }
    collection = db['sg_otu_detail']
    name_dic = dict()
    for col in collection.find({"otu_id": ObjectId(data)}):
        new_classify_name = ""
        for i in range(1, level + 1):
            my_list = list()
            my_str = col[LEVEL[i]]
            if not my_str:
                my_str = LEVEL[i] + "no_rank"
            my_list.append(my_str)
        new_classify_name = ";".join(my_list)
        if new_classify_name not in name_dic:
            name_dic[new_classify_name] = dict()
            for sp in samples:
                name_dic[new_classify_name][sp] = int(col[sp])
        else:
            for sp in samples:
                name_dic[new_classify_name][sp] += int(col[sp])
    with open(file_path, "wb") as f:
        f.write("OTU ID\t%s\ttaxonomy\n" % "\t".join(samples))
        for k in name_dic.iterkeys():
            line = k
            for s in samples:
                line += "\t" + str(name_dic[k][s])
            line += k + "\n"
            f.write(line)
    return file_path


def export_group_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    group_table = db['sg_specimen_group']
    sample_table = db['sg_specimen_group']
    group_name_list = list()
    group_name = bind_obj.sheet.option("category_name")
    group_name_list = re.split(',', group_name)
    with open(file_path, "wb") as f:
        group_schema = group_table.fine_one({"_id": ObjectId(data)})
        c_name = group_schema["category_names"]
        length = len(c_name)
        index_list = list()
        for i in range(length):
            for g_name in group_name_list:
                if g_name == c_name[i]:
                    index_list.append(i)
                    break
        sample_id = defaultdict(list)
        sample_name = defaultdict(list)
        for i in index_list:
            sample_id[c_name[i]] = group_schema['group_schema'][i].keys()
        for k in sample_id:
            value = sample_id[k]
            for v in value:
                result = sample_table.fine_one({"_id": ObjectId(v)})
                sample_name['k'].append(result['specimen_name'])
        for k in sample_name:
            for i in range(len(sample_name[k])):
                f.write("{}\t{}\n".format(k, sample_name[k][i]))
