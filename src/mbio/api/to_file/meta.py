# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
import copy
import json
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
    """
    按等级获取OTU表
    """
    file_path = os.path.join(dir_path, "%s_input.otu.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    bind_obj.logger.debug("samples1")
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    samples = list()
    for result in results:
        samples.append(result['specimen_name'])
    bind_obj.logger.debug("samples")
    bind_obj.logger.debug(samples)
    level = int(bind_obj.sheet.option("level"))
    collection = db['sg_otu_detail']
    name_dic = dict()
    for col in collection.find({"otu_id": ObjectId(data)}):
        tmp = level + 1
        new_classify_name = _create_classify_name(col, tmp)
        if new_classify_name not in name_dic:
            name_dic[new_classify_name] = dict()
            for sp in samples:
                name_dic[new_classify_name][sp] = int(col[sp])
        else:
            for sp in samples:
                name_dic[new_classify_name][sp] += int(col[sp])
    with open(file_path, "wb") as f:
        f.write("OTU ID\t%s\n" % "\t".join(samples))
        for k in name_dic.iterkeys():
            line = k
            for s in samples:
                line += "\t" + str(name_dic[k][s])
            line += "\n"
            f.write(line)
    return file_path


def _create_classify_name(col, tmp):
    """
    在数据库读取OTU表的分类信息，形成OTU表的第一列
    """
    LEVEL = {
        1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
        6: "f__", 7: "g__", 8: "s__", 9: "otu"
    }
    my_list = list()
    last_classify = ""
    for i in range(1, tmp):
        if LEVEL[i] not in col:
            print LEVEL[i]
            print last_classify
            if last_classify == "":
                tmp = col[LEVEL[i - 1]]
                last_classify = re.split('__', tmp)[1]
            my_str = LEVEL[i] + last_classify + "__unclasified"
        else:
            my_str = col[LEVEL[i]]
        my_list.append(my_str)
    new_classify_name = ";".join(my_list)
    return new_classify_name


def export_group_table(data, option_name, dir_path, bind_obj=None):
    """
    按group_id 和 组名获取group表
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    group_table = db['sg_specimen_group']
    sample_table = db['sg_specimen']
    group_name_list = list()
    group_name = bind_obj.sheet.option("category_name")
    group_name_list = re.split(',', group_name)
    with open(file_path, "wb") as f:
        group_schema = group_table.find_one({"_id": ObjectId(data)})
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
            sample_id[c_name[i]] = group_schema['specimen_names'][i].keys()
        for k in sample_id:
            value = sample_id[k]
            for v in value:
                result = sample_table.find_one({"_id": ObjectId(v)})
                if not result:
                    raise Exception("无法根据传入的group_id:{}找到相应的样本名".format(data))
                sample_name[k].append(result['specimen_name'])
        for k in sample_name:
            for i in range(len(sample_name[k])):
                f.write("{}\t{}\n".format(k, sample_name[k][i]))


def export_group_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按分组的详细信息获取group表
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    if not isinstance(data, dict):
        try:
            table_dict = json.loads(data)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    with open(file_path, "wb") as f:
        for k in table_dict:
            for sp in table_dict[k]:
                f.write("{}\t{}\n".format(k, sp))
    return file_path


def export_distance_matrix(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.matrix.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的距离矩阵为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_beta_specimen_distance_detail']
    results = collection.find({"specimen_distance_id": data})
    samples = []
    copy_results = copy.deepcopy(results)
    bind_obj.logger.info(str(results))
    for result in results:
        samples.append(result["specimen_name"])
    bind_obj.logger.info('ALL SAMPLE:' + ' '.join(samples))
    copysamples = copy.deepcopy(samples)
    with open(file_path, "wb") as f:
        f.write("\t%s\n" % "\t".join(samples))
        for sample in samples:
            doc = {}
            value = []
            for result in copy_results:
                if result['specimen_name'] == sample:
                    doc = result
                    break
            for detail in copysamples:
                value.append(doc[detail])
            value = [str(i) for i in value]
            f.write(sample + '\t' + '\t'.join(value) + '\n')
    return file_path
