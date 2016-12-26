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
db = client[Config().MONGODB]


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
    # 因为有些样本名以1,2,3,4进行编号， 导致读出来了之后samples列表里的元素是数字， 需要先转化成字符串
    samples = map(str, samples)
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
            line += col["s__"]
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
    # 因为有些样本名以1,2,3,4进行编号， 导致读出来了之后samples列表里的元素是数字， 需要先转化成字符串
    samples = map(str, samples)
    level = int(bind_obj.sheet.option("level"))
    collection = db['sg_otu_detail']
    name_dic = dict()
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_detail表中未找到！".format(data))
    for col in results:
        tmp = level + 1
        new_classify_name = _create_classify_name(col, tmp, bind_obj)
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


def _create_classify_name(col, tmp, bind_obj):
    LEVEL = {
        1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
        6: "f__", 7: "g__", 8: "s__", 9: "otu"
    }
    for i in range(1, 10):
        if LEVEL[i] not in col:
            raise Exception("Mongo数据库中的taxonomy信息不完整")
    new_col = list()
    for i in range(1, tmp):
        new_col.append(col[LEVEL[i]])
    return "; ".join(new_col)


def _get_only_classify_name(col, level, bind_obj):
    LEVEL = {
        1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
        6: "f__", 7: "g__", 8: "s__", 9: "otu"
    }
    if level in LEVEL:
        if LEVEL[level] in col:
            return col[LEVEL[level]]
        else:
            raise Exception('数据库中不存在列：{}'.format(LEVEL[level]))
    else:
        raise Exception('错误的分类水平：{}'.format(level))


"""
def _create_classify_name(col, tmp, bind_obj):
    LEVEL = {
        1: "d__", 2: "k__", 3: "p__", 4: "c__", 5: "o__",
        6: "f__", 7: "g__", 8: "s__", 9: "otu"
    }
    new_cla = list()
    if "d__" not in col:
        raise Exception("在域水平上分类学缺失")
    # if re.search("uncultured$", col["d__"]) or re.search("Incertae_Sedis$", col["d__"]) or re.search("norank$", col["d__"]) or re.search("unidentified$", col["d__"]):
    if re.search("(uncultured|Incertae_Sedis|norank|unidentified|Unclassified)$", col["d__"], flags=re.I):
        pass
        # raise Exception("在域水平上的分类为uncultured或Incertae_Sedis或norank或是unidentified")
    # 先对输入的名字进行遍历， 当在某一水平(输入的level之前)上空着的时候， 补全
    # 例如在g水平空着的时候，补全成g__unidentified
    for i in range(1, tmp):
        if LEVEL[i] in col:
            new_cla.append(col[LEVEL[i]])
        else:
            str_ = LEVEL[i] + "Unclassified"
            new_cla.append(str_)

    # 对uncultured，Incertae_Sedis，norank，unidentified进行补全
    claList = list()
    for i in range(tmp - 1):
        my_tmp = re.split('__', new_cla[i])
        if len(my_tmp) > 1:
            claList.append([(my_tmp[0], my_tmp[1])])
        else:
            claList.append(["", my_tmp[0]])
    bind_obj.logger.info(claList)
    for i in range(1, tmp - 1):
        cla = claList[i][0][1]
        if re.search("(uncultured|Incertae_Sedis|norank|unidentified|Unclassified)", cla, flags=re.I):
            j = i - 1
            while (j >= 1):
                last_cla = claList[j][0][1]
                if last_cla != cla:
                    claList[i].extend(claList[j])
                    j = j - 1
                    break
                j = j - 1
    tax_list = list()
    for i in range(0, tmp - 1):
        tmp_tax = list()
        for j in range(0, len(claList[i])):
            my_tax = "{}__{}".format(claList[i][j][0], claList[i][j][1])
            tmp_tax.append(my_tax)
        tax_list.append("_".join(tmp_tax))

    new_classify_name = "; ".join(tax_list)
    return new_classify_name
"""


def export_group_table(data, option_name, dir_path, bind_obj=None):
    """
    按group_id 和 组名获取group表
    使用时确保你的workflow的option里category_name这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    if data in ["all", "All", "ALL"]:
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
    """
    获取specimen_names字段的index (key)
    """
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
    if data in ["all", "All", "ALL"]:
        with open(file_path, "wb") as f:
            f.write("#sample\t" + "##empty_group##" + "\n")
        return file_path
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
    schema_name = re.sub("\s", "_", group_schema["group_name"])  # 将分组方案名的空格替换成下划线
    with open(file_path, "wb") as f:
        f.write("#sample\t" + schema_name + "\n")

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
    使用时确保你的workflow的option里group_detail这个字段
    """
    file_path = os.path.join(dir_path, "%s_input.group.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的GROUP表格为文件，路径:%s" % (option_name, file_path))
    data = _get_objectid(data)
    group_detail = bind_obj.sheet.option('group_detail')
    second_group_detail = bind_obj.sheet.option('second_group_detail')
    group_table = db['sg_specimen_group']
    if not isinstance(group_detail, dict):
        try:
            table_list = [json.loads(group_detail)]
        except Exception:
            raise Exception("生成group表失败，传入的一级分组不是一个字典或者是字典对应的字符串")
    if second_group_detail != '' and not isinstance(second_group_detail, dict):
        try:
            table_list.append(json.loads(second_group_detail))
        except Exception:
            raise Exception("生成group表失败，传入的二级分组不是一个字典或者是字典对应的字符串")
    # bind_obj.logger.debug('{}'.format(table_list))
    for i in table_list:
        if not isinstance(i, dict):
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
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


def export_otu_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按等级与分组信息(group_detail)获取OTU表
    使用时确保你的workflow的option里level与group_detail这个字段
    """
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    bind_obj.logger.debug(data)
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_specimen表中未找到！".format(data))
    samples = list()
    table_dict = {}
    group_detail = bind_obj.sheet.option("group_detail")
    bind_obj.logger.debug(group_detail)
    if not isinstance(group_detail, dict):
        try:
            table_dict = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    sample_table = db['sg_specimen']
    for k in table_dict:
        for sp_id in table_dict[k]:
            sp = sample_table.find_one({"_id": ObjectId(sp_id)})
            if not sp:
                raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(sp_id, 'sg_specimen'))
            else:
                samples.append(sp["specimen_name"])

    level = int(bind_obj.sheet.option("level"))
    collection = db['sg_otu_detail']
    name_dic = dict()
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_detail表中未找到！".format(data))
    for col in results:
        tmp = level + 1
        new_classify_name = _create_classify_name(col, tmp, bind_obj)
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


def export_otu_table_without_zero(data, option_name, dir_path, bind_obj=None):
    """
    按等级与分组信息(group_detail)获取OTU表
    使用时确保你的workflow的option里level与group_detail这个字段
    """
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    bind_obj.logger.debug(data)
    collection = db['sg_otu_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_specimen表中未找到！".format(data))
    samples = list()
    table_dict = {}
    group_detail = bind_obj.sheet.option("group_detail")
    bind_obj.logger.debug(group_detail)
    if not isinstance(group_detail, dict):
        try:
            table_dict = json.loads(group_detail)
        except Exception:
            raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    if not isinstance(table_dict, dict):
        raise Exception("生成group表失败，传入的{}不是一个字典或者是字典对应的字符串".format(option_name))
    sample_table = db['sg_specimen']
    for k in table_dict:
        for sp_id in table_dict[k]:
            sp = sample_table.find_one({"_id": ObjectId(sp_id)})
            if not sp:
                raise Exception("group_detal中的样本_id:{}在样本表{}中未找到".format(sp_id, 'sg_specimen'))
            else:
                samples.append(sp["specimen_name"])

    level = int(bind_obj.sheet.option("level"))
    collection = db['sg_otu_detail']
    name_dic = dict()
    results = collection.find({"otu_id": ObjectId(data)})
    if not results.count():
        raise Exception("otu_id: {}在sg_otu_detail表中未找到！".format(data))
    for col in results:
        tmp = level + 1
        new_classify_name = _create_classify_name(col, tmp, bind_obj)
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
            line_data = map(lambda x: float(x), line.strip("\n").split("\t")[1:])
            if not any(line_data):
                continue
            else:
                f.write(line)
    return file_path
