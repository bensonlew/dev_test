# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'
import os
import re
import json
from types import StringTypes
from biocluster.config import Config
from bson.objectid import ObjectId
from collections import defaultdict


def export_sample_list(file_list, option_name, dir_path, bind_obj=None):
    client = Config().mongo_client
    db = client["samplebase"]
    file_path = os.path.join(dir_path, "%s.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_test_batch_specimen']
    my_collection = db['sg_test_specimen']
    f = open(file_path, "wb")
    f.write(
        "#ID\tSample\talias_name\tplatform\tstrategy\tprimer\tcontract_number\tcontract_sequence_number\tmj_number\tclient_name\tsequence_num\tbase_num\tmean_length\tmin_length\tmax_length\tfile_name\n")
    for key in file_list:
        batch_specimen_result = collection.find_one({"_id": ObjectId(key)})
        print batch_specimen_result
        specimen_id = batch_specimen_result["specimen_id"]
        Sample_name = batch_specimen_result["alias_name"]
        info = my_collection.find_one({"_id": ObjectId(specimen_id)})
        if not info:
            raise Exception("意外错误，样本id:{}在sg_test_specimen中未找到！")
         # 如果该序列是重组所得，则该file_name存放文件路径的list,不是重组，则len(list)==1
        file_name = ";".join(info['file_path'])
        new_line = str(specimen_id) + '\t' + str(Sample_name) + '\t' + str(file_list[key]) + '\t' + str(info['platform'])+ '\t' + \
                   str(info['strategy']) + '\t' + str(info['primer']) + '\t' + \
                   str(info['contract_number']) + '\t' + str(info['contract_sequence_number']) + '\t' + \
                   str(info['mj_number']) + '\t' +  str(info['client_name']) + '\t' + str(info['sequence_num']) + '\t' + \
                   str(info['base_num']) + '\t' + str(info['mean_length']) + '\t' + str(info['min_length']) + '\t' + str(info['max_length']) +\
                   '\t' + str(file_name) + '\n'
        f.write(new_line)
    f.close()
    return file_path

if __name__ == '__main__':
    file_list = {"59e047fea4e1af2f0178dd94": "Test", "59e047fda4e1af2f0178dd8c": "Test",
                 "59e047fda4e1af2f0178dd86": "Con", "59e047fea4e1af2f0178dd92": "Con"}
    a = export_sample_list(file_list, "sample_info", "/mnt/ilustre/users/sanger-dev/sg-users/wangzhaoyue/sample_base")

