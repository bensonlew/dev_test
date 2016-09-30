# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import os
from biocluster.config import Config
from bson.objectid import ObjectId
import types


client = Config().mongo_client
db = client[Config().MONGODB] + '_rna'


def export_express_matrix(data, option_name, dir_path, bind_obj=None):
    fpkm_path = os.path.join(dir_path, "%s_fpkm.matrix" % option_name)
    count_path = os.path.join(dir_path, "%s_count.matrix" % option_name)
    bind_obj.logger.debug("正在导出计数矩阵:%s；fpkm矩阵:%s" % (count_path, fpkm_path))
    collection = db['sg_denovo_express_detail']
    my_collection = db['sg_denovo_express']
    results = collection.find({'$and': [{'express_id': ObjectId(data)}, {'type': 'gene'}]})
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，express_id:{}在sg_denovo_express中未找到！".format(ObjectId(data)))
    samples = my_result['specimen'].split(',')
    with open(fpkm_path, "wb") as f, open(count_path, 'wb') as c:
        head = '\t'.join(samples)
        f.write('\t' + head + '\n')
        c.write('\t' + head + '\n')
        for result in results:
            gene_id = result['gene_id']
            fpkm_write = '{}'.format(gene_id)
            count_write = '{}'.format(gene_id)
            for sam in samples:
                fpkm = sam + '_fpkm'
                count = sam + '_count'
                fpkm_write += '\t{}'.format(result[fpkm])
                count_write += '\t{}'.format(result[count])
            fpkm_write += '\n'
            count_write += '\n'
            f.write(fpkm_write)
            c.write(count_write)
    paths = ','.join(fpkm_path, count_path)
    return paths


def export_control_file(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, '{}.txt'.format(option_name))
    bind_obj.logger.debug("正在导出计数矩阵:%s" % file_path)
    collection = db['sg_denovo_control']
    result = collection.find_one({'_id': ObjectId(data)})
    if not result:
        raise Exception("意外错误，control_id:{}在sg_denovo_control中未找到！".format(ObjectId(data)))
    group_id = result['group_id']
    if isinstance(group_id, types.StringTypes):
        group_id = ObjectId(group_id)
    group_coll = db['sg_denovo_specimen_group']
    g_result = group_coll.find_one({'_id': group_id})
    if not g_result:
        raise Exception("意外错误，control_file的group_id:{}在sg_denovo_specimen_group中未找到！".format(group_id))
    control_detail = result['control_names']
    with open(file_path, 'wb') as w:
        w.write('#control\t{}\n'.format(result['scheme_name']))
        for i in control_detail:
            w.write('{}\t{}\n'.format(i, control_detail[i]))
    return file_path
