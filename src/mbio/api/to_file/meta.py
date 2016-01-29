# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
import copy
import json
from biocluster.config import Config
from bson.objectid import ObjectId


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
    """
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
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
    group_schema = group_table.find_one({"_id": ObjectId(data)})
    c_name = group_schema["category_names"]
    # 当group表里的那条记录有"otu_id"字段时, 去sg_otu_specimen表里由id去找样本名
    # 当group表里的那条记录有"task_id"字段时, 去sg_specimen表里由id去找样本名
    if "otu_id" in group_schema:
        sample_table = db['sg_otu_specimen']
    elif "task_id" in group_schema:
        sample_table = db['sg_specimen']
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
            raise Exception("无法根据传入的group_id:{}找到相应的样本名".format(data))
        sample_name[result['specimen_name']] = sample_id[k]
    with open(file_path, "wb") as f:
        for k in sample_name:
            for i in range(len(sample_name[k])):
                f.write("{}\t{}\n".format(sample_name[k], k))


def _get_index_list(group_name_list, c_name):
    length = len(c_name)
    index_list = list()
    for i in range(length):
        for g_name in group_name_list:
            if g_name == c_name[i]:
                index_list.append(i)
                break
    return index_list


def export_group_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按分组的详细信息获取group表
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    bind_obj.logger.info(data)
    if not isinstance(data, dict):
        try:
            table_dict = json.loads(data)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的table_dict不是一个字典或者是字典对应的字符串".format(option_name))
    with open(file_path, "wb") as f:
        for k in table_dict:
            for sp in table_dict[k]:
                f.write("{}\t{}\n".format(sp, k))
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
