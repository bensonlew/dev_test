# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
# import copy
import json
from types import StringTypes
from biocluster.config import Config
from bson.objectid import ObjectId
from collections import defaultdict


client = Config().mongo_client
db = client["sanger"]


def export_otu_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
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
        for col in collection.find({"otu_id": ObjectId(data)}):
            line = "%s\t" % col["otu"]
            for s in samples:
                line += "%s\t" % col[s]
            for cls in ["d__", "k__", "p__", "c__", "o__", "f__", "g__"]:
                if cls in col.keys():
                    line += "%s; " % col[cls]
            f.write("%s\n" % line)
    return file_path


def export_otu_table_by_level(data, option_name, dir_path, bind_obj=None):
    """
    按等级获取OTU表
    使用时确保你的workflow的option里level这个字段
    """
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_specimen表中未找到！".format(data))
    samples = list()
    for result in results:
        samples.append(result['specimen_name'])
    level = int(bind_obj.sheet.option("level"))
    collection = db['sg_otu_detail']
    name_dic = dict()
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_detail表中未找到！".format(data))
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
            if last_classify == "":
                last_classify = col[LEVEL[i - 1]]
            my_str = LEVEL[i] + "Unclasified_" + last_classify
        else:
            if not col[LEVEL[i]]:
                if LEVEL[i] == "otu":
                    my_str = "otu__" + "Unclasified_" + last_classify
                else:
                    my_str = LEVEL[i] + "Unclasified_" + last_classify
            else:
                my_str = col[LEVEL[i]]
        my_list.append(my_str)
    new_classify_name = "; ".join(my_list)
    return new_classify_name


def export_group_table(data, option_name, dir_path, bind_obj=None):
    """
    按group_id 和 组名获取group表
    使用时确保你的workflow的option里category_name这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    group_table = db['sg_specimen_group']
    group_name_list = list()
    group_name = bind_obj.sheet.option("category_name")
    group_name_list = re.split(',', group_name)
    data = _get_objectid(data)
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_specimen_group表里找到相应的记录".format(data))
    c_name = group_schema["category_names"]
    sample_table_name = _get_sample_table(group_schema)
    sample_table = db[sample_table_name]
    index_list = _get_index_list(group_name_list, c_name)
    sample_id = dict()
    sample_name = dict()
    for i in index_list:
        for k in group_schema['specimen_names'][i]:
            # k 样品ID
            sample_id[k] = c_name[i]
    for k in sample_id:
        result = sample_table.find_one({"_id": ObjectId(k)})
        if not result:
            raise Exception("无法根据传入的group_id:{}在样本表{}里找到相应的样本名".format(data, sample_table_name))
        sample_name[result['specimen_name']] = sample_id[k]

    my_data = bind_obj.sheet.data
    if "pan_id" in my_data['options']:
        sample_number_check(sample_name)

    with open(file_path, "wb") as f:
        for k in sample_name:
            f.write("{}\t{}\n".format(k, sample_name[k]))
    return file_path


def _get_sample_table(group_schema):
    # 当group表里的那条记录有"otu_id"字段时, 去sg_otu_specimen表里由id去找样本名
    # 当group表里的那条记录有"task_id"字段时, 去sg_specimen表里由id去找样本名
    if "otu_id" in group_schema:
        sample_table_name = "sg_otu_specimen"
    elif "task_id" in group_schema:
        sample_table_name = 'sg_specimen'
    return sample_table_name


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


def _get_index_list(group_name_list, c_name):
    length = len(c_name)
    index_list = list()
    for i in range(length):
        for g_name in group_name_list:
            if g_name == c_name[i]:
                index_list.append(i)
                break
    return index_list


def sample_number_check(sample_name):
    sample_count = defaultdict(int)
    for k in sample_name:
        sample_count[sample_name[k]] += 1
    for k in sample_count:
        if sample_count[k] < 3:
            raise Exception("组{}里的样本数目小于三个，每个组里必须有三个以上的样本".format(k))


def export_group_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按分组的详细信息获取group表
    使用时确保你的workflow的option里group_detal这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    data = _get_objectid(data)
    group_detail = bind_obj.sheet.option('group_detail')
    group_table = db['sg_specimen_group']
    if not isinstance(group_detail, dict):
        print group_detail
        print type(group_detail)
        try:
            table_dict = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_specimen_group表里找到相应的记录".format(data))
    sample_table_name = _get_sample_table(group_schema)
    sample_table = db[sample_table_name]

    with open(file_path, "wb") as f:
        for k in table_dict:
            for sp_id in table_dict[k]:
                sp = sample_table.find_one({"_id": ObjectId(sp_id)})
                if not sp:
                    raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(sp_id, sample_table_name))
                else:
                    sp_name = sp["specimen_name"]
                f.write("{}\t{}\n".format(sp_name, k))
    return file_path
