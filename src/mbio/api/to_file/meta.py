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
    my_collection = db['sg_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    samples = []
    for result in results:
        id_result = my_collection.find_one({"_id": result["specimen_id"]})
        if not id_result:
            raise Exception("意外错误，样本id:{}在sg_specimen中未找到！")
        samples.append(id_result["specimen_name"])
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
        if "specimen_id" not in result:
            raise Exception("otu_id:{}错误，请使用新导入的OTU表的id".format(data))
        sp_id = result['specimen_id']
        my_collection = db['sg_specimen']
        my_result = my_collection.find_one({"_id": sp_id})
        if not my_result:
            raise Exception("意外错误，样本id:{}在sg_specimen表里未找到".format(sp_id))
        samples.append(my_result["specimen_name"])
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
    for i in range(1, 10):
        if LEVEL[i] in col:
            if re.search("uncultured$", col[LEVEL[i]]) or re.search("Incertae_Sedis$", col[LEVEL[i]]) or re.search("norank$", col[LEVEL[i]]):
                if i == 0:
                    raise Exception("在域水平上的分类为uncultured或Incertae_Sedis或是norank")
                else:
                    col[LEVEL[i]] = col[LEVEL[i]] + "_" + col[LEVEL[i - 1]]
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
    if data == "all":
        with open(file_path, "wb") as f:
            f.write("#sample\t" + "##empty_group##" + "\n")
        return file_path
    group_table = db['sg_specimen_group']
    group_name_list = list()
    group_name = bind_obj.sheet.option("category_name")
    group_name_list = re.split(',', group_name)
    data = _get_objectid(data)
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_specimen_group表里找到相应的记录".format(data))
    c_name = group_schema["category_names"]

    with open(file_path, "wb") as f:
        f.write("#sample\t" + group_schema["group_name"] + "\n")

    sample_table_name = 'sg_specimen'
    sample_table = db[sample_table_name]
    index_list = _get_index_list(group_name_list, c_name)
    sample_id = list()  # [[id名,组名], [id名, 组名]，...]
    sample_name = list()  # [[样本名, 组名], [样本名, 组名], ...]
    for i in index_list:
        for k in group_schema['specimen_names'][i]:
            # k 样品ID
            sample_id.append([k, c_name[i]])
    for pair in sample_id:
        result = sample_table.find_one({"_id": ObjectId(pair[0])})
        if not result:
            raise Exception("无法根据传入的group_id:{}在样本表{}里找到相应的样本名".format(data, sample_table_name))
        sample_name.append([result['specimen_name'], pair[1]])

    my_data = bind_obj.sheet.data
    if "pan_id" in my_data['options']:
        sample_number_check(sample_name)

    with open(file_path, "ab") as f:
        for pair in sample_name:
            f.write("{}\t{}\n".format(pair[0], pair[1]))
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
    for pair in sample_name:
        sample_count[pair[1]] += 1
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
        try:
            table_dict = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_specimen_group表里找到相应的记录".format(data))

    with open(file_path, "wb") as f:
        f.write("#sample\t" + group_schema["group_name"] + "\n")

    sample_table_name = 'sg_specimen'
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


def export_cascading_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    根据group_detail生成group表或者二级group表
    使用时确保你的workflow的option里group_detal这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    data = _get_objectid(data)
    group_detail = bind_obj.sheet.option('group_detail')
    group_table = db['sg_specimen_group']
    if not isinstance(group_detail, list):
        try:
            table_list = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个数组或者是数组对应的字符串".format(option_name))
    if not isinstance(table_list, list):
        raise Exception("生成group表失败，传入的{}不是一个数组或者是数组对应的字符串".format(option_name))
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    if not group_schema:
        raise Exception("无法根据传入的group_id:{}在sg_specimen_group表里找到相应的记录".format(data))

    with open(file_path, "wb") as f:
        f.write("#sample\t" + group_schema["group_name"])

    sample_table_name = 'sg_specimen'
    sample_table = db[sample_table_name]
    _write_cascading_table(table_list, sample_table, file_path, sample_table_name)
    return file_path


def _write_class1_table(table_list, file_path, sample_table, sample_table_name):
    table_dict = table_list[0]
    with open(file_path, "ab") as f:
        f.write("\n")
        for k in table_dict:
            for sp_id in table_dict[k]:
                sp = sample_table.find_one({"_id": ObjectId(sp_id)})
                if not sp:
                    raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(sp_id, sample_table_name))
                else:
                    sp_name = sp["specimen_name"]
                f.write("{}\t{}\n".format(sp_name, k))


def _write_class2_table(table_list, file_path, sample_table, sample_table_name):
    dict1 = dict()  # 样本id -> 分组名
    dict2 = dict()
    for k in table_list[0]:
        for id_ in table_list[0][k]:
            if id_ in dict1:
                raise Exception("样本id:{}在分组{}，{}中都出现，一个样本在同一级别上只能属于一个分组！".format(id_, dict1[id_], k))
            dict1[id_] = k
    for k in table_list[1]:
        for id_ in table_list[1][k]:
            if id_ not in dict1:
                raise Exception("样本id:{}在第一级的分组中未出现".format(id_))
            if id_ in dict2:
                raise Exception("样本id:{}在分组{}，{}中都出现，一个样本在同一级别上只能属于一个分组！".format(id_, dict1[id_], k))
            dict2[id_] = k
    if len(dict1) != len(dict2):
        raise Exception("一级分组中的某些样本id在二级分组中未找到！")
    with open(file_path, "ab") as f:
        f.write("\tsecond_group\n")
        for k in dict1:
            sp = sample_table.find_one({"_id": ObjectId(k)})
            if not sp:
                raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(k, sample_table_name))
            else:
                sp_name = sp["specimen_name"]
            f.write("{}\t{}\t{}\n".format(sp_name, dict1[k], dict2[k]))


def _write_cascading_table(table_list, sample_table, file_path, sample_table_name):
    length = len(table_list)
    if length == 1:
        _write_class1_table(table_list, file_path, sample_table, sample_table_name)
    elif length == 2:
        _write_class2_table(table_list, file_path, sample_table, sample_table_name)
    else:
        raise Exception("group_detal字段含有三个或以上的字典")
