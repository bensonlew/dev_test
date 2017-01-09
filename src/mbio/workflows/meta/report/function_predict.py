# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
import json
from types import StringTypes
from biocluster.config import Config
from bson.objectid import ObjectId
from collections import defaultdict
import pymongo


client = Config().mongo_client
db = client[Config().MONGODB]


def export_otu_table_by_detail(data, option_name, dir_path, bind_obj=None):
    """
    按分组信息(group_detail)获取OTU表
    使用时确保你的workflow的option里group_detail这个字段
    """
    table_path = os.path.join(dir_path, "otu_table.xls")
    rep_path = os.path.join(dir_path, "otu_reps.fasta")
    bind_obj.logger.debug("正在导出OTU表格文件，路径:%s" % (table_path))
    bind_obj.logger.debug("正在导出OTU表格文件，路径:%s" % (rep_path))
    my_collection = db['sg_otu_specimen']
    my_results = my_collection.find({"otu_id": ObjectId(data)})
    if not my_results.count():
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
    collection = db['sg_otu_detail']
    with open(table_path, "wb") as f, open(rep_path, "wb") as w:
        f.write("OTU ID\t%s\n" % "\t".join(samples))
        for col in collection.find({"otu_id": ObjectId(data)}):
            table_line = "%s\t" % col["otu"]
            for s in samples:
                table_line += "%s\t" % col[s]
            f.write("%s\n" % table_line)
            line = ">%s\n" % col["otu"]
            line += col["otu_rep"]
            w.write("%s\n" % line)
    paths = ','.join([table_path, rep_path])
    return paths
