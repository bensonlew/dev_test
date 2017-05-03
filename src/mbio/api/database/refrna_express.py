#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__: konghualei 20170424

from pymongo import MongoClient
from bson.objectid import ObjectId
import types
from types import StringTypes
import re
import json, time
import pandas as pd
import numpy as np
import datetime, os
from bson.son import SON
from collections import Counter
import glob
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config

class RefrnaExpress(Base):
    def __init__(self, bind_object):
        super(RefrnaExpress, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'
    
    @report_check
    def add_express(self, rsem_dir=None, transcript_fasta_path=None, is_duplicate=None, class_code = None, samples=None, params=None, name=None, express_diff_id=None, bam_path=None, major=True, distri_path = None):
            # task_id ="tsg_1000"
            # project_sn = "10000951"
            task_id = self.bind_object.sheet.id
            project_sn = self.bind_object.sheet.project_sn
            # if not express_diff_id:
                # params={"value_type":value_type, "query_type":query_type,"method":method, "group_id":group_id,"group_detail":group_detail}
            insert_data = {
                'project_sn': project_sn,
                'task_id': task_id,
                'name': name if name else 'ExpressStat_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
                'desc': '表达量计算主表',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'params': (json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params, dict) else params),
                'specimen': samples,
                'status': 'end',
                'bam_path': bam_path,
                'transcript_fasta_path': transcript_fasta_path,
                'is_duplicate': is_duplicate
            }
            if params:
                insert_data["genes"]=True
                if params["express_method"] == "rsem":
                    insert_data["trans"]=True
                if params["express_method"].lower() == "featurecounts":
                    insert_data["trans"]=False
            if express_diff_id:
                insert_data["express_diff_id"] = express_diff_id
            collection = db['sg_express']
            express_id = collection.insert_one(insert_data).inserted_id
            print "插入主表id是{}".format(express_id)
            value_type = params["type"]
            method = params["express_method"]
            # express_id=ObjectId("58f03a28a4e1af44d4139c79")
            if major:
                rsem_files = os.listdir(rsem_dir)
                sample_group = "sample"
                for f in rsem_files:
                    if re.search(r'^genes\.TMM', f):
                        fpkm_path = rsem_dir + "/" + f
                        count_path = rsem_dir + '/genes.counts.matrix'
                        self.add_express_detail(express_id, count_path, fpkm_path, class_code, 'gene', value_type, method, sample_group)
                        self.add_express_gragh(express_id, distribution_path_log2 = distri_path+"/log2gene_distribution.xls", \
                                          distribution_path_log10 = distri_path+"/log10gene_distribution.xls", \
                                          distribution_path = distri_path+"/gene_distribution.xls", sample_group = "sample", query_type="gene")
                        self.add_express_gragh(express_id, distribution_path_log2 = distri_path+"/log2GroupGenes_distribution.xls", \
                                          distribution_path_log10 = distri_path+"/log10GroupGenes_distribution.xls", \
                                          distribution_path = distri_path+"/GroupGenes_distribution.xls", sample_group = "group", query_type="gene")
                        self.add_express_box(express_id, fpkm_path = os.path.split(rsem_dir)[0]+"/oldrsem/"+f, sample_group="sample", query_type="gene")
                        self.add_express_box(express_id, fpkm_path=distri_path+"/group/Group.genes_genes.TMM.fpkm.matrix", sample_group="group", query_type="gene")
                    elif re.search(r'^transcripts\.TMM', f):
                        fpkm_path = rsem_dir + "/" + f
                        count_path = rsem_dir + '/transcripts.counts.matrix'
                        self.add_express_detail(express_id, count_path, fpkm_path, class_code, 'transcript', value_type, method, sample_group)
                        self.add_express_gragh(express_id, distribution_path_log2 = distri_path+"/log2transcript_distribution.xls", \
                                          distribution_path_log10 = distri_path+"/log10transcript_distribution.xls", \
                                          distribution_path = distri_path+"/transcript_distribution.xls", sample_group = "sample", query_type="transcript")
                        self.add_express_gragh(express_id, distribution_path_log2 = distri_path+"/log2GroupTrans_distribution.xls", \
                                          distribution_path_log10 = distri_path+"/log10GroupTrans_distribution.xls", \
                                          distribution_path = distri_path+"/GroupTrans_distribution.xls", sample_group = "group", query_type="transcript")
                        self.add_express_box(express_id, fpkm_path = os.path.split(rsem_dir)[0]+"/oldrsem/"+f, sample_group="sample", query_type="transcript")
                        self.add_express_box(express_id, fpkm_path=distri_path+"/group/Group.trans_transcripts.TMM.fpkm.matrix", sample_group="group", query_type="transcript")
                    elif re.search(r'\.genes\.results$', f):
                        sample = f.split('.genes.results')[0]
                        file_ = rsem_dir + "/" + f
                        self.add_express_specimen_detail(express_id, file_, 'gene', sample)
                    elif re.search(r'\.isoforms\.results$', f):
                        sample = f.split('.isoforms.results')[0]
                        file_ = rsem_dir + "/" + f
                        self.add_express_specimen_detail(express_id, file_, 'transcript', sample)
            return express_id
    
    @report_check
    def add_express_detail(self, express_id, count_path, fpkm_path, class_code=None, query_type=None, value_type=None, method=None, sample_group=None, diff=True,):
            if not isinstance(express_id, ObjectId):
                if isinstance(express_id, types.StringTypes):
                    express_id = ObjectId(express_id)
                else:
                    raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
            if not os.path.exists(count_path):
                raise Exception('count_path:{}所指定的路径不存在，请检查！'.format(count_path))
            if not os.path.exists(fpkm_path):
                raise Exception('fpkm_path:{}所指定的路径不存在，请检查！'.format(fpkm_path))
            def class_code_get(class_code, query_type):
                if class_code:
                    if os.path.exists(class_code):
                        with open(class_code, 'r+') as cc:
                            class_code_dict ={}
                            for lines in cc:
                                line = lines.strip().split("\t")
                                if query_type == "gene":
                                    if line[1] not in class_code_dict.keys():
                                        class_code_dict[line[1]] = str(line[2])
                                    else:
                                        pass
                                if query_type == "transcript":
                                    if line[0] not in class_code_dict.keys():
                                        class_code_dict[line[0]] = str(line[2])
                                    else:
                                        pass
                        return class_code_dict
            data_list = list()
            count_dict = {}
            sample_count = {}
            if not diff:
                class_code_info = class_code_get(class_code = class_code, query_type = query_type)
            with open(count_path, 'rb') as c, open(fpkm_path, 'rb') as f:
                for sam in samples:
                    sample_count[sam] = 0
                c.readline()
                for line in c:
                    line = line.strip().split('\t')
                    count_dict[line[0]] = line[1:]
                    count = line[1:]
                    for i in range(len(count)):
                        if float(count[i]) > 0:
                            sample_count[samples[i]] += 1
                f.readline()
                for l in f:
                    l = l.strip().split('\t')
                    seq_id = l[0]
                    if re.search(r'(,)', seq_id):
                        sequence_id = seq_id.split(",")[0] #以 ',' 为分隔符切割序列id和gene_name
                        gene_name = seq_id.split(",")[1]
                    else:
                        sequence_id = seq_id
                        gene_name = None
                    fpkm = l[1:]
                    if not diff:
                        if sequence_id in class_code_info.keys():
                            _class_code = class_code_info[sequence_id]
                            if _class_code != "=":
                                _class=True
                            else:
                                _class=False
                        else:
                            _class=None
                            # raise Exception("{}class_code信息没有找到，请确认:{}!".format(seq_id,class_code))
                    else:
                        _class = None
                    data = [
                        ('seq_id', sequence_id),
                        ('type', query_type),
                        ('express_id', express_id),
                        ("value_type", value_type),
                        ("is_new", _class),
                        ("method", method),
                        ("gene_name", gene_name), #添加gene_name信息
                        ("sample_group", sample_group)
                    ]
                    for i in range(len(samples)):
                        data += [
                            ('{}_count'.format(samples[i]), float(count_dict[seq_id][i])), ('{}_fpkm'.format(samples[i]), float(fpkm[i])),
                            ('{}_sum'.format(samples[i]), sample_count[samples[i]]),
                        ]
                    data = SON(data)
                    data_list.append(data)
            try:
                collection = db["sg_express_detail"]
                collection.insert_many(data_list)
            except Exception, e:
                # bind_object.logger.error("导入表达量矩阵信息出错:%s" % e)
                print ("导入表达量矩阵信息出错:%s" % e)
            else:
                print ("导入表达量矩阵信息成功!")
                # bind_object.logger.info("导入表达量矩阵信息成功!")
    
    @report_check
    def add_express_gragh(self, express_id, distribution_path_log2, distribution_path_log10, distribution_path, sample_group, query_type=None):    
        def stat(fpkm_data,density,log=None):
            tmp = []
            if len(fpkm_data) != len(density):
                raise Exception("density必须和fpkm长度相等！")
            else:
                for i in range(len(fpkm_data)):
                    if not log:
                        tmp.append({"fpkm":round(fpkm_data[i],6), "density": round(density[i], 6)})
                return tmp
        dflog2 = pd.read_table(distribution_path_log2)
        dflog10 = pd.read_table(distribution_path_log10)
        df = pd.read_table(distribution_path)
        samples = df.columns[1:]
        data_list = []
        for i in samples:
            insert_data = [
                ('express_id', express_id),
                ('type', query_type),
                ('specimen', i),
                ('sample_group', sample_group)
            ]
            tmp = stat(fpkm_data = df["fpkm"],density = df[i])
            tmplog2 = stat(fpkm_data = dflog2["log2fpkm"], density = dflog2[i])
            tmplog10 = stat(fpkm_data = dflog10["log10fpkm"], density = dflog10[i])
            insert_data.append(('data', tmp))
            insert_data.append(('data_log2', tmplog2))
            insert_data.append(('data_log10', tmplog10))
            insert_data = SON(insert_data)
            data_list.append(insert_data)
        try:
            collection = db["sg_express_gragh"]
            collection.insert_many(data_list)
        except Exception, e:
            print ("导入表达量矩阵作图数据：%s信息出错:%s" % (distribution_path_log2, e))
        else:
            print ("导入表达量矩阵作图数据: %s信息成功!" % distribution_path_log2)           
    
    @report_check
    def add_express_box(self, express_id, fpkm_path, sample_group, query_type=None):
        def log_value(value, log):
            """获取log值"""
            if log == 2:
                return np.log2(value.apply(lambda x: x+1))
            elif log == 10:
                return np.log10(value.apply(lambda x: x+1))
            else:
                return value

        def box_info(fpkm, samples, log = None):
            box = {}
            gene_list = {}
            for sam in samples:
                gene_list[sam] = {}
                box[sam] = {}
                min = log_value(fpkm[sam], log).min()
                max = log_value(fpkm[sam], log).max()
                q1 = log_value(fpkm[sam], log).quantile(0.25)
                q3 = log_value(fpkm[sam], log).quantile(0.75)
                median = log_value(fpkm[sam],log).median()
                box[sam]={"min": min, "max": max,'q1': q1,'q3': q3,'median': median}
                min_q1 = fpkm[[0]][log_value(fpkm[sam], log).apply(lambda x: x>=min and x<=q1)].values
                gene_list[sam]['min-q1'] = [i[0] for i in min_q1]
                q1_median = fpkm[[0]][log_value(fpkm[sam], log).apply(lambda x: x>q1 and x<=median)].values
                gene_list[sam]['q1-median'] = [i[0] for i in q1_median]
                median_q3 = fpkm[[0]][log_value(fpkm[sam], log).apply(lambda x: x>median and x<=q3)].values
                gene_list[sam]['median-q3'] = [i[0] for i in median_q3]
                q3_max = fpkm[[0]][log_value(fpkm[sam], log).apply(lambda x: x>q3 and x<=max)].values
                gene_list[sam]['q3-max'] = [i[0] for i in q3_max]
            return box, gene_list
        express_info =db["sg_express"].find_one({"_id": ObjectId(express_id)})
        files = open(fpkm_path,'r+')
        samples = files.readline().strip().split("\t")[1:]

        fpkm = pd.read_table(fpkm_path)
        box = {}
        log2box = {}
        log10box = {}
        gene_list = {}
        log2gene_list = {}
        log10gene_list = {}
        data = [
            ("express_id", express_id),
            ("sample_group", sample_group),
            ("type", query_type)
        ]

        box, gene_list = box_info(fpkm = fpkm, samples = samples)
        log2box, log2gene_list = box_info(fpkm = fpkm, log = 2, samples = samples)
        log10box, log10gene_list = box_info(fpkm = fpkm, log = 10, samples = samples)
        for sam in samples:
            data += [
                (sam,[
                  {'{}'.format(sam): box[sam]}, {'{}_log2'.format(sam): log2box[sam]},{'{}_log10'.format(sam): log10box[sam]}
                ]),
                ('{}_gene_list'.format(sam), [
                {'{}'.format(sam): gene_list[sam]},{'{}_log2'.format(sam): log2gene_list[sam]},{'{}_log10'.format(sam): log10gene_list[sam]}
                ])
            ]
        data = SON(data)
        id = db['sg_express_box'].insert_one(data).inserted_id
        print id
    
    @report_check
    def add_express_specimen_detail(self, express_id, rsem_result, rsem_type, sample=None):
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(rsem_result):
            raise Exception('rsem_result所指定的路径：{}不存在，请检查！'.format(rsem_result))
        sample_name = os.path.basename(rsem_result).split('.')[0]
        data_list = []
        with open(rsem_result, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('express_id', express_id),
                    ('specimen_name', sample if sample else sample_name),
                    ('type', rsem_type),
                    ('length', float(line[2])),
                    ('effective_length', float(line[3])),
                    ('expected_count', float(line[4])),
                    ('TPM', round(float(line[5]), 4)),
                    ('FPKM', round(float(line[6]), 4)),
                ]
                if rsem_type == 'gene':
                    data += [
                        ('gene_id', line[0]),
                        ('transcript_id', line[1]),
                    ]
                else:
                    data += [
                        ('gene_id', line[1]),
                        ('transcript_id', line[0]),
                        ('IsoPct', float(line[7])),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = db["sg_denovo_express_specimen_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print ("导入单样本表达量矩阵：%s信息出错:%s" % (rsem_result, e))
        else:
            print ("导入单样本表达量矩阵: %s信息成功!" % rsem_result)

    
if __name__ == "__main__":
    db = MongoClient("192.168.10.189:27017").tsanger_ref_rna
    transcript_fasta_path = "/mnt/ilustre/users/sanger-dev/workspace/20170410/Single_assembly_module_tophat_stringtie_zebra/Assembly/assembly_newtranscripts/merged.fa"
    # rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_7/Express/output/rsem"
    # class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_7/Express/output/class_code"
    rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/rsem"
    class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/class_code"
    samples=["ERR1621569","ERR1621480","ERR1621658","ERR1621391"]
    is_duplicate = True
    params={}
    params["express_method"]="rsem"
    params["type"]="fpkm"
    params["group_id"] = "58f01bbca4e1af488e52de3d"
    params["group_detail"] = {"A":["58d8a96e719ad0adae70fa14","58d8a96e719ad0adae70fa12"],
                               "B":["58d8a96e719ad0adae70fa11", "58d8a96e719ad0adae70fa13"]}
    distri_path = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/MergeRsem"
    data = RefrnaExpress()
    data.add_express(rsem_dir=rsem_dir, transcript_fasta_path=transcript_fasta_path, is_duplicate=is_duplicate, class_code = class_code, samples=samples, \
        params=params, name=None, express_diff_id=None, bam_path=None, major=True, distri_path = distri_path)
    # rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/oldrsem"
    
    # add_express(rsem_dir=rsem_dir, transcript_fasta_path=None, is_duplicate=is_duplicate, class_code = None, samples=samples, \
        # params=params, name=None, express_diff_id=, bam_path=None, major=True, distri_path = None)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # data = db["sg_specimen"].find({"task_id":"tsg_1000"})
    # specimen_info = []
    # samples_A =["ERR1621569","ERR1621480"]
    # samples_B = ["ERR1621658","ERR1621391"] 
    # specimen_add = []
    # for d in data:
        # specimen = d["specimen_name"]
        # if specimen not in specimen_add:
            # specimen_add.append(specimen)
            # specimen_info.append({str(d["_id"]):specimen})
    # print specimen_info
    # print specimen_add
    # opts = {
        # "task_id":"tsg_1000",
        # "category_names":specimen_add,
        # "specimen_names":specimen_info,
        # "group_name":"test",
        # "project_sn":None
    # }
    # opts=SON(opts)
    # group_id = db["sg_specimen_group"].insert_one(opts).inserted_id
    # print group_id
    
    