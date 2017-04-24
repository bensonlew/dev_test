# -*- coding: utf-8 -*-
# __author__ = 'zhaoyue.wang'
import json
import datetime
import os
import re
from biocluster.api.database.base import Base, report_check
from bson.son import SON
import types
from bson.objectid import ObjectId
import pymongo
from biocluster.config import Config


class RefAssembly(object):
    def __init__(self):
        super(RefAssembly, self).__init__()
        self.db = pymongo.MongoClient(host="192.168.10.189", port=27017).tsanger_ref_rna
# class RefAssembly(Base):
#     def __init__(self, bind_object):
#         super(RefAssembly, self).__init__(bind_object)
#         self._db_name = Config().MONGODB + '_ref_rna'

    # @report_check
    def add_assembly_result(self, name=None, params=None, all_gtf_path=None, merged_path=None,
                            old_gene_trans_file_1=None,old_gene_trans_file_5=None,old_gene_trans_file_10=None,old_gene_trans_file_20=None,
                            old_trans_exon_file_1=None,old_trans_exon_file_5=None,old_trans_exon_file_10=None,old_trans_exon_file_20=None,
                            new_gene_trans_file_1=None,new_gene_trans_file_5=None,new_gene_trans_file_10=None,new_gene_trans_file_20=None,
                            new_trans_exon_file_1=None,new_trans_exon_file_5=None,new_trans_exon_file_10=None,new_trans_exon_file_20=None):
        # task_id = self.bind_object.sheet.id
        # project_sn = self.bind_object.sheet.project_sn
        task_id = "tsg_1000"
        project_sn = "10000000"
        merged_list = []
        merged_gtf_path = dict()
        merged_gtf_path["gtf"] = merged_path + '/merged.gtf'
        merged_list.append(merged_gtf_path)
        merged_fa_path = dict()
        merged_fa_path["fa"] = merged_path + '/merged.fa'
        merged_list.append(merged_fa_path)
        files_list = [old_gene_trans_file_1, old_gene_trans_file_5, old_gene_trans_file_10, old_gene_trans_file_20,
                      old_trans_exon_file_1, old_trans_exon_file_5, old_trans_exon_file_10, old_trans_exon_file_20,
                      new_gene_trans_file_1, new_gene_trans_file_5, new_gene_trans_file_10, new_gene_trans_file_20,
                      new_trans_exon_file_1, new_trans_exon_file_5, new_trans_exon_file_10, new_trans_exon_file_20]
        name_list = ['old_gene_trans_data', 'old_gene_trans_data_5', 'old_gene_trans_data_10', 'old_gene_trans_data_20',
                     'old_exon_trans_data', 'old_exon_trans_data_5', 'old_exon_trans_data_10', 'old_exon_trans_data_20',
                     'new_gene_trans_data', 'new_gene_trans_data_5', 'new_gene_trans_data_10', 'new_gene_trans_data_20',
                     'new_exon_trans_data', 'new_exon_trans_data_5', 'new_exon_trans_data_10', 'new_exon_trans_data_20']
        dic = {}
        for i in range(len(files_list)):
            f = files_list[i]

            with open(f, "r") as fr:
                dic[i] = []
                for line in fr:
                    gene_trans_dic = dict()
                    lines = line.strip().split("\t")
                    gene_trans_dic[lines[0]] = lines[1]
                    dic[i].append(gene_trans_dic)
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'Assembly_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'params': params,
            'status': 'end',
            'desc': '拼接组装结果主表',
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'step': [200, 300, 600, 1000],
            'seq_type': ["i", "j", "o", "u", "x"],
            'merge_path': merged_list,
            'Sample_gtf_path': all_gtf_path,

        }
        collection = self.db['sg_transcripts']
        transcript_id = collection.insert_one(insert_data).inserted_id
        for i in range(len(files_list)):
            collection.update({'_id': ObjectId(transcript_id)}, {'$set': {name_list[i]: dic[i]}})
        return transcript_id

    def add_transcripts_step(self, transcript_id, Statistics_path):

        if not isinstance(transcript_id, ObjectId):
            if isinstance(transcript_id, types.StringTypes):
                transcript_id = ObjectId(transcript_id)
            else:
                raise Exception('transcript_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(Statistics_path):
            raise Exception('{}所指定的路径不存在，请检查！'.format(Statistics_path))

        step_files = os.listdir(Statistics_path)
        data_list = []
        for f in step_files:
            m = re.search(r'trans_count_stat_(\S+)\.txt', f)
            if m:
                step_list = []
                step = m.group(1)
                files = os.path.join(Statistics_path, f)
                fr = open(files, "r")
                next(fr)
                for line in fr:
                    step_dic = dict()
                    step_range = line.strip().split("\t")[0]
                    num = line.strip().split("\t")[1]
                    step_dic[step_range] = num
                    step_list.append(step_dic)
                data = [
                    ('transcripts_id', transcript_id),
                    ('step', int(step)),
                    ('step_data', step_list)
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_transcripts_step']
            collection.insert_many(data_list)
        except Exception:
            print ("失败")
            # self.bind_object.logger.error("导入步长%s信息失败!" % (Statistics_path))
        else:
            print ("成功")
            # self.bind_object.logger.info("导入步长%s信息成功!" % (Statistics_path))

    def add_transcripts_seq_type(self, transcript_id, code_file):
        if not isinstance(transcript_id, ObjectId):
            if isinstance(transcript_id, types.StringTypes):
                transcript_id = ObjectId(transcript_id)
            else:
                raise Exception('transcript_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(code_file):
            raise Exception('{}所指定的文件不存在，请检查！'.format(code_file))
        data_list = []
        code_list = []
        with open(code_file, "r") as fr:
            for line in fr:
                lines = line.strip().split("\t")
                gene_list = lines[1].strip().split(",")
                code_list.append(lines[0])
                data = [
                    ('transcripts_id', transcript_id),
                    ('class_code', lines[0]),
                    ('num', int(lines[2])),
                    ('gene_list', gene_list),
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db['sg_transcripts_seq_type']
            collection.insert_many(data_list)
            collection = self.db['sg_transcripts']
            collection.update({'_id': ObjectId(transcript_id)},  {'$set':{'seq_type': code_list}})
        except Exception:
            print ("失败")
            # self.bind_object.logger.error("导入class_code信息：%s失败!" % (code_file))
        else:
            print ("成功")
            # self.bind_object.logger.info("导入class_code信息：%s成功!" % (code_file))

if __name__ == "__main__":
    a = RefAssembly()
    all_gtf_path = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Stringtie'
    merged_path = "/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/StringtieMerge"
    old_gene_trans_file_1 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_genes.gtf.trans_1.txt'
    old_gene_trans_file_5 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_genes.gtf.trans_5.txt'
    old_gene_trans_file_10 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_genes.gtf.trans_10.txt'
    old_gene_trans_file_20 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_genes.gtf.trans_20.txt'

    old_trans_exon_file_1 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_trans.gtf.exon_1.txt'
    old_trans_exon_file_5 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_trans.gtf.exon_5.txt'
    old_trans_exon_file_10 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_trans.gtf.exon_10.txt'
    old_trans_exon_file_20 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/old_trans.gtf.exon_20.txt'

    new_gene_trans_file_1 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_genes.gtf.trans_1.txt'
    new_gene_trans_file_5 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_genes.gtf.trans_5.txt'
    new_gene_trans_file_10 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_genes.gtf.trans_10.txt'
    new_gene_trans_file_20 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_genes.gtf.trans_20.txt'

    new_trans_exon_file_1 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_transcripts.gtf.exon_1.txt'
    new_trans_exon_file_5 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_transcripts.gtf.exon_5.txt'
    new_trans_exon_file_10 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_transcripts.gtf.exon_10.txt'
    new_trans_exon_file_20 = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/new_transcripts.gtf.exon_20.txt'

    transcript_id = a.add_assembly_result(all_gtf_path=all_gtf_path, merged_path=merged_path,
                                          old_gene_trans_file_1=old_gene_trans_file_1,
                                          old_gene_trans_file_5=old_gene_trans_file_5,
                                          old_gene_trans_file_10=old_gene_trans_file_10,
                                          old_gene_trans_file_20=old_gene_trans_file_20,
                                          old_trans_exon_file_1=old_trans_exon_file_1,
                                          old_trans_exon_file_5=old_trans_exon_file_5,
                                          old_trans_exon_file_10=old_trans_exon_file_10,
                                          old_trans_exon_file_20=old_trans_exon_file_20,
                                          new_gene_trans_file_1=new_gene_trans_file_1,
                                          new_gene_trans_file_5=new_gene_trans_file_5,
                                          new_gene_trans_file_10=new_gene_trans_file_10,
                                          new_gene_trans_file_20=new_gene_trans_file_20,
                                          new_trans_exon_file_1=new_trans_exon_file_1,
                                          new_trans_exon_file_5=new_trans_exon_file_5,
                                          new_trans_exon_file_10=new_trans_exon_file_10,
                                          new_trans_exon_file_20=new_trans_exon_file_20)
    Statistics_path = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics'
    a.add_transcripts_step(transcript_id=transcript_id, Statistics_path=Statistics_path)
    code_files = '/mnt/ilustre/users/sanger-dev/workspace/20170411/Single_assembly_module_tophat_stringtie_zebra_2/Assembly/output/Statistics/code_num.txt'
    a.add_transcripts_seq_type(transcript_id=transcript_id, code_file=code_files)