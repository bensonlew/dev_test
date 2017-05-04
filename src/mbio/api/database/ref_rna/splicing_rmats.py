# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/5/3 09:34

import re, os, Bio, argparse, sys, fileinput, urllib2
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId
import bson.binary
from cStringIO import StringIO
import re
import json
import pandas as pd
import numpy as np
from collections import Counter

from pymongo import MongoClient
import json, datetime

RMATS_DETAIL_TABLE_HEAD = ["event_id", "type", "novel", "old", "gene", "gene_symbol", "chr", "strand",
                           "exonStart_0base", "exonEnd", "upstreamES", "upstreamEE", "downstreamES", "downstreamEE",
                           "longExonStart_0base", "longExonEnd", "shortES", "shortEE", "flankingES", "flankingEE",
                           "riExonStart_0base", "riExonEnd", "1stExonStart_0base", "1stExonEnd", "2ndExonStart_0base",
                           "2ndExonEnd",
                           "diff_JunctionCountOnly", "IJC_SAMPLE_1", "SJC_SAMPLE_1", "IJC_SAMPLE_2", "SJC_SAMPLE_2",
                           "IncFormLen_JunctionCountOnly", "SkipFormLen_JunctionCountOnly", "PValue_JunctionCountOnly",
                           "FDR_JunctionCountOnly", "IncLevel1_JunctionCountOnly", "IncLevel2_JunctionCountOnly",
                           "average_IncLevel1_JunctionCountOnly", "average_IncLevel2_JunctionCountOnly",
                           "IncLevelDifference_JunctionCountOnly", "increase_inclusion_SAMPLE1_JunctionCountOnly",
                           "increase_exclusion_SAMPLE1_JunctionCountOnly",
                           "increase_inclusion_SAMPLE2_JunctionCountOnly",
                           "increase_exclusion_SAMPLE2_JunctionCountOnly",
                           "diff_ReadsOnTargetAndJunctionCounts", "IC_SAMPLE_1", "SC_SAMPLE_1", "IC_SAMPLE_2",
                           "SC_SAMPLE_2",
                           "IncFormLen_ReadsOnTargetAndJunctionCounts", "SkipFormLen_ReadsOnTargetAndJunctionCounts",
                           "PValue_ReadsOnTargetAndJunctionCounts", "FDR_ReadsOnTargetAndJunctionCounts",
                           "IncLevel1_ReadsOnTargetAndJunctionCounts", "IncLevel2_ReadsOnTargetAndJunctionCounts",
                           "average_IncLevel1_ReadsOnTargetAndJunctionCounts",
                           "average_IncLevel2_ReadsOnTargetAndJunctionCounts",
                           "IncLevelDifference_ReadsOnTargetAndJunctionCounts",
                           "increase_inclusion_SAMPLE1_ReadsOnTargetAndJunctionCounts",
                           "increase_exclusion_SAMPLE1_ReadsOnTargetAndJunctionCounts",
                           "increase_inclusion_SAMPLE2_ReadsOnTargetAndJunctionCounts",
                           "increase_exclusion_SAMPLE2_ReadsOnTargetAndJunctionCounts",
                           "diff_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts",
                           "diff_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts"]

RMATS_DETAIL_MONGO_TABLE_FIELD_DIC = {'event_id': 'event_id', 'type': 'type', 'novel_as': 'novel', 'gid': 'gene',
                                      'gname': 'gene_symbol',
                                      'chr': 'chr',
                                      'strand': 'strand', 'es': 'exonStart_0base', 'ee': 'exonEnd',
                                      'up_es': 'upstreamES',
                                      'up_ee': 'upstreamEE',
                                      'down_es': 'downstreamES', 'down_ee': 'downstreamEE',
                                      'les': 'longExonStart_0base',
                                      'lee': 'longExonEnd',
                                      'ses': 'shortES', 'see': 'shortEE', 'fes': 'flankingES', 'fee': 'flankingEE',
                                      'ries': 'riExonStart_0base',
                                      'riee': 'riExonEnd', 'firstes': '1stExonStart_0base', 'firstee': '1stExonEnd',
                                      'secondes': '2ndExonStart_0base',
                                      'secondee': '2ndExonEnd', 'diff_jc': 'diff_JunctionCountOnly',
                                      'ijc_s1': 'IJC_SAMPLE_1',
                                      'sjc_s1': 'SJC_SAMPLE_1',
                                      'ijc_s2': 'IJC_SAMPLE_2', 'sjc_s2': 'SJC_SAMPLE_2',
                                      'inclen_jc': 'IncFormLen_JunctionCountOnly',
                                      'skiplen_jc': 'SkipFormLen_JunctionCountOnly',
                                      'pvalue_jc': 'PValue_JunctionCountOnly',
                                      'fdr_jc': 'FDR_JunctionCountOnly', 'inc1_jc': 'IncLevel1_JunctionCountOnly',
                                      'inc2_jc': 'IncLevel2_JunctionCountOnly',
                                      'aver_inc1_jc': 'average_IncLevel1_JunctionCountOnly',
                                      'aver_inc2_jc': 'average_IncLevel2_JunctionCountOnly',
                                      'inc_diff_jc': 'IncLevelDifference_JunctionCountOnly',
                                      'upinc_s1_jc': 'increase_inclusion_SAMPLE1_JunctionCountOnly',
                                      'upexc_s1_jc': 'increase_exclusion_SAMPLE1_JunctionCountOnly',
                                      'upinc_s2_jc': 'increase_inclusion_SAMPLE2_JunctionCountOnly',
                                      'upexc_s2_jc': 'increase_exclusion_SAMPLE2_JunctionCountOnly',
                                      'diff_all': 'diff_ReadsOnTargetAndJunctionCounts',
                                      'ic_s1': 'IC_SAMPLE_1', 'sc_s1': 'SC_SAMPLE_1', 'ic_s2': 'IC_SAMPLE_2',
                                      'sc_s2': 'SC_SAMPLE_2',
                                      'inclen_all': 'IncFormLen_ReadsOnTargetAndJunctionCounts',
                                      'skiplen_all': 'SkipFormLen_ReadsOnTargetAndJunctionCounts',
                                      'pvalue_all': 'PValue_ReadsOnTargetAndJunctionCounts',
                                      'fdr_all': 'FDR_ReadsOnTargetAndJunctionCounts',
                                      'inc1_all': 'IncLevel1_ReadsOnTargetAndJunctionCounts',
                                      'inc2_all': 'IncLevel2_ReadsOnTargetAndJunctionCounts',
                                      'aver_inc1_all': 'average_IncLevel1_ReadsOnTargetAndJunctionCounts',
                                      'aver_inc2_all': 'average_IncLevel2_ReadsOnTargetAndJunctionCounts',
                                      'inc_diff_all': 'IncLevelDifference_ReadsOnTargetAndJunctionCounts',
                                      'upinc_s1_all': 'increase_inclusion_SAMPLE1_ReadsOnTargetAndJunctionCounts',
                                      'upexc_s1_all': 'increase_exclusion_SAMPLE1_ReadsOnTargetAndJunctionCounts',
                                      'upinc_s2_all': 'increase_inclusion_SAMPLE2_ReadsOnTargetAndJunctionCounts',
                                      'upexc_s2_all': 'increase_exclusion_SAMPLE2_ReadsOnTargetAndJunctionCounts',
                                      'diff_jc_and_all': 'diff_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts',
                                      'diff_jc_or_all': 'diff_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts'}

RMATS_STATS_MONGO_TABLE_FIELD_DIC = {'total_jc': 'total_JunctionCountOnly_event_id_set_no',
                                     'as_total': 'total_as_events_no',
                                     'total_all': 'total_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'as_novel': 'total_as_novel_events_no',
                                     'all_a5ss': 'A5SS_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'jcandall_a5ss': 'A5SS_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts_set_no',
                                     'old_a5ss': 'A5SS_old_event_id_set_no',
                                     'jc_a5ss': 'A5SS_JunctionCountOnly_event_id_set_no',
                                     'novel_a5ss': 'A5SS_novel_event_id_no',
                                     'total_a5ss': 'A5SS_all_event_id_no',
                                     'jcorall_a5ss': 'A5SS_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts_set_no',
                                     'all_se': 'SE_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'jcandall_se': 'SE_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts_set_no',
                                     'old_se': 'SE_old_event_id_set_no',
                                     'jc_se': 'SE_JunctionCountOnly_event_id_set_no',
                                     'novel_se': 'SE_novel_event_id_no',
                                     'total_se': 'SE_all_event_id_no',
                                     'jcorall_se': 'SE_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts_set_no',
                                     'all_mxe': 'MXE_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'jcandall_mxe': 'MXE_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts_set_no',
                                     'old_mxe': 'MXE_old_event_id_set_no',
                                     'jc_mxe': 'MXE_JunctionCountOnly_event_id_set_no',
                                     'novel_mxe': 'MXE_novel_event_id_no',
                                     'total_mxe': 'MXE_all_event_id_no',
                                     'jcorall_mxe': 'MXE_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts_set_no',
                                     'all_ri': 'RI_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'jcandall_ri': 'RI_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts_set_no',
                                     'old_ri': 'RI_old_event_id_set_no',
                                     'jc_ri': 'RI_JunctionCountOnly_event_id_set_no',
                                     'novel_ri': 'RI_novel_event_id_no',
                                     'total_ri': 'RI_all_event_id_no',
                                     'jcorall_ri': 'RI_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts_set_no',
                                     'all_a3ss': 'A3SS_ReadsOnTargetAndJunctionCounts_event_id_set_no',
                                     'jcandall_a3ss': 'A3SS_JunctionCountOnly_and_ReadsOnTargetAndJunctionCounts_set_no',
                                     'old_a3ss': 'A3SS_old_event_id_set_no',
                                     'jc_a3ss': 'A3SS_JunctionCountOnly_event_id_set_no',
                                     'novel_a3ss': 'A3SS_novel_event_id_no',
                                     'total_a3ss': 'A3SS_all_event_id_no',
                                     'jcorall_a3ss': 'A3SS_JunctionCountOnly_or_ReadsOnTargetAndJunctionCounts_set_no'}

RMATS_PSI_MONGO_TABLE_FIELD_DIC = {'a3ss_s1_all_exc': 'A3SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'a3ss_s1_all_inc': 'A3SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'a3ss_s1_all_total': 'A3SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   'a3ss_s1_jc_exc': 'A3SS_SAMPLE_1_JunctionCountOnly_exclusion',
                                   'a3ss_s1_jc_inc': 'A3SS_SAMPLE_1_JunctionCountOnly_inclusion',
                                   'a3ss_s1_jc_total': 'A3SS_SAMPLE_1_JunctionCountOnly_total',
                                   'a3ss_s2_all_exc': 'A3SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'a3ss_s2_all_inc': 'A3SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'a3ss_s2_all_total': 'A3SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   'a3ss_s2_jc_exc': 'A3SS_SAMPLE_2_JunctionCountOnly_exclusion',
                                   'a3ss_s2_jc_inc': 'A3SS_SAMPLE_2_JunctionCountOnly_inclusion',
                                   'a3ss_s2_jc_total': 'A3SS_SAMPLE_2_JunctionCountOnly_total',
                                   'a5ss_s1_all_exc': 'A5SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'a5ss_s1_all_inc': 'A5SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'a5ss_s1_all_total': 'A5SS_SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   'a5ss_s1_jc_exc': 'A5SS_SAMPLE_1_JunctionCountOnly_exclusion',
                                   'a5ss_s1_jc_inc': 'A5SS_SAMPLE_1_JunctionCountOnly_inclusion',
                                   'a5ss_s1_jc_total': 'A5SS_SAMPLE_1_JunctionCountOnly_total',
                                   'a5ss_s2_all_exc': 'A5SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'a5ss_s2_all_inc': 'A5SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'a5ss_s2_all_total': 'A5SS_SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   'a5ss_s2_jc_exc': 'A5SS_SAMPLE_2_JunctionCountOnly_exclusion',
                                   'a5ss_s2_jc_inc': 'A5SS_SAMPLE_2_JunctionCountOnly_inclusion',
                                   'a5ss_s2_jc_total': 'A5SS_SAMPLE_2_JunctionCountOnly_total',
                                   'mxe_s1_all_exc': 'MXE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'mxe_s1_all_inc': 'MXE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'mxe_s1_all_total': 'MXE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   'mxe_s1_jc_exc': 'MXE_SAMPLE_1_JunctionCountOnly_exclusion',
                                   'mxe_s1_jc_inc': 'MXE_SAMPLE_1_JunctionCountOnly_inclusion',
                                   'mxe_s1_jc_total': 'MXE_SAMPLE_1_JunctionCountOnly_total',
                                   'mxe_s2_all_exc': 'MXE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'mxe_s2_all_inc': 'MXE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'mxe_s2_all_total': 'MXE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   'mxe_s2_jc_exc': 'MXE_SAMPLE_2_JunctionCountOnly_exclusion',
                                   'mxe_s2_jc_inc': 'MXE_SAMPLE_2_JunctionCountOnly_inclusion',
                                   'mxe_s2_jc_total': 'MXE_SAMPLE_2_JunctionCountOnly_total',
                                   'ri_s1_all_exc': 'RI_SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'ri_s1_all_inc': 'RI_SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'ri_s1_all_total': 'RI_SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   'ri_s1_jc_exc': 'RI_SAMPLE_1_JunctionCountOnly_exclusion',
                                   'ri_s1_jc_inc': 'RI_SAMPLE_1_JunctionCountOnly_inclusion',
                                   'ri_s1_jc_total': 'RI_SAMPLE_1_JunctionCountOnly_total',
                                   'ri_s2_all_exc': 'RI_SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'ri_s2_all_inc': 'RI_SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'ri_s2_all_total': 'RI_SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   'ri_s2_jc_exc': 'RI_SAMPLE_2_JunctionCountOnly_exclusion',
                                   'ri_s2_jc_inc': 'RI_SAMPLE_2_JunctionCountOnly_inclusion',
                                   'ri_s2_jc_total': 'RI_SAMPLE_2_JunctionCountOnly_total',
                                   'se_s1_all_exc': 'SE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'se_s1_all_inc': 'SE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'se_s1_all_total': 'SE_SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   'se_s1_jc_exc': 'SE_SAMPLE_1_JunctionCountOnly_exclusion',
                                   'se_s1_jc_inc': 'SE_SAMPLE_1_JunctionCountOnly_inclusion',
                                   'se_s1_jc_total': 'SE_SAMPLE_1_JunctionCountOnly_total',
                                   'se_s2_all_exc': 'SE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion',
                                   'se_s2_all_inc': 'SE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion',
                                   'se_s2_all_total': 'SE_SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   'se_s2_jc_exc': 'SE_SAMPLE_2_JunctionCountOnly_exclusion',
                                   'se_s2_jc_inc': 'SE_SAMPLE_2_JunctionCountOnly_inclusion',
                                   'se_s2_jc_total': 'SE_SAMPLE_2_JunctionCountOnly_total',
                                   's1_all_exc_total': 'SAMPLE_1_ReadsOnTargetAndJunctionCounts_exclusion_total',
                                   's1_all_inc_total': 'SAMPLE_1_ReadsOnTargetAndJunctionCounts_inclusion_total',
                                   's1_all_total': 'SAMPLE_1_ReadsOnTargetAndJunctionCounts_total',
                                   's1_jc_exc_total': 'SAMPLE_1_JunctionCountOnly_exclusion_total',
                                   's1_jc_inc_total': 'SAMPLE_1_JunctionCountOnly_inclusion_total',
                                   's1_jc_total': 'SAMPLE_1_JunctionCountOnly_total',
                                   's2_all_exc_total': 'SAMPLE_2_ReadsOnTargetAndJunctionCounts_exclusion_total',
                                   's2_all_inc_total': 'SAMPLE_2_ReadsOnTargetAndJunctionCounts_inclusion_total',
                                   's2_all_total': 'SAMPLE_2_ReadsOnTargetAndJunctionCounts_total',
                                   's2_jc_exc_total': 'SAMPLE_2_JunctionCountOnly_exclusion_total',
                                   's2_jc_inc_total': 'SAMPLE_2_JunctionCountOnly_inclusion_total',
                                   's2_jc_total': 'SAMPLE_2_JunctionCountOnly_total'
                                   }


class TextLineIterator(object):
    def __init__(self, path):
        self.path = path
    
    def __iter__(self):
        for line in open(self.path):
            yield line


def isnumber(aString):
    try:
        float(aString)
        int(aString)
        return True
    except:
        return False


# noinspection PyUnboundLocalVariable
if __name__ == '__main__':
    rmats_detail_file_head_index_dic = dict(zip(RMATS_DETAIL_TABLE_HEAD, range(64)))
    params = {
        "group_id": "1111122222",
        "ana_mode": "P",
        "read_len": 150,
        "novel": 1,
        "lib_type": "fr-unstranded",
        "as_diff": 0.001,
        "seq_type": "paired"
    }
    
    insert_data = {
        'project_sn': 'sn_10000',
        'task_id': 'tsanger_12345',
        'name': 'test_splicing_main_table_3',
        'desc': '可变剪接rmats计算主表',
        'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'params': (
            json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params, dict) else params),
        'status': 'end',
    }
    
    client = MongoClient('mongodb://10.100.200.124:27017/')
    db = client['tsanger_ref_rna']
    collection = db['sg_splicing_rmats']

    splicing_id = collection.insert_one(insert_data).inserted_id
    rmats_detail_data_lst = []
    rmats_detail_collection = db['sg_splicing_rmats_detail']
    rmats_detail_file = '/mnt/ilustre/users/sanger-dev/workspace/20170502/' \
                        'Single_rmats_module_linfang_new/Rmats/RmatsBam/output/all_events_detail_big_table.txt'

    rmats_detail_file_content = open(rmats_detail_file)
    rmats_detail_file_content.readline()
    while 1:
        line = rmats_detail_file_content.readline()
        if not line.strip():
            break
        data = dict()
        arr = re.split('\t+', line.strip())
        if len(arr) != 64:
            raise Exception(
                'line in rmats big detail file %s is not legal: not 64 columns: %s' % (rmats_detail_file, line))
        data['splicing_id'] = splicing_id
        for field in RMATS_DETAIL_MONGO_TABLE_FIELD_DIC.keys():
            data[field] = arr[rmats_detail_file_head_index_dic[RMATS_DETAIL_MONGO_TABLE_FIELD_DIC[field]]]
        rmats_detail_data_lst.append(data)

    try:
        rmats_detail_collection.insert_many(rmats_detail_data_lst)
        print("导入rmats事件详情表：%s信息成功:%s" % (rmats_detail_file))
    except Exception, e:
        print("导入rmats事件详情表：%s信息出错:%s" % (rmats_detail_file, e))
    
    # ==========导入
    rmats_stats_collection = db['sg_splicing_rmats_stats']
    rmats_stats_file = '/mnt/ilustre/users/sanger-dev/workspace/20170502/' \
                       'Single_rmats_module_linfang_new/Rmats/RmatsBam/output/event_stats.file.txt'
    rmats_stats_dic = dict(
        [(arr[0], arr[1]) for arr in [line.strip().split('\t') for line in open(rmats_stats_file).readlines()]])
    rmats_stats_data = dict()
    for field in RMATS_STATS_MONGO_TABLE_FIELD_DIC.keys():
        rmats_stats_data[field] = int(rmats_stats_dic[RMATS_STATS_MONGO_TABLE_FIELD_DIC[field]])

    rmats_stats_data['as_old'] = int(rmats_stats_data['as_total']) - int(rmats_stats_data['as_novel'])
    rmats_stats_data['total_ jcandall'] = sum(
        [rmats_stats_data['jcandall_mxe'], rmats_stats_data['jcandall_se'], rmats_stats_data['jcandall_ri'],
         rmats_stats_data['jcandall_a3ss'], rmats_stats_data['jcandall_a5ss']])
    rmats_stats_data['total_ jcorall'] = sum(
        [rmats_stats_data['jcorall_mxe'], rmats_stats_data['jcorall_se'], rmats_stats_data['jcorall_ri'],
         rmats_stats_data['jcorall_a3ss'], rmats_stats_data['jcorall_a5ss']])
    rmats_stats_data['splicing_id'] = splicing_id

    try:
        rmats_stats_collection.insert_one(rmats_stats_data)
        print("导入rmats事件统计表：%s信息成功" % (rmats_stats_file))
    except Exception, e:
        print("导入rmats事件统计表：%s信息出错:%s" % (rmats_stats_file, e))

    
    
    # =============导psi表
    rmats_psi_collection = db['sg_splicing_rmats_psi']
    rmats_psi_file = '/mnt/ilustre/users/sanger-dev/workspace/20170502/' \
                     'Single_rmats_module_linfang_new/Rmats/RmatsBam/output/psi_stats.file.txt'
    rmats_psi_dic = dict(
        [(arr[0], arr[1]) for arr in [line.strip().split('\t') for line in open(rmats_psi_file).readlines()]])
    rmats_psi_data = dict()
    for field in RMATS_PSI_MONGO_TABLE_FIELD_DIC.keys():
        try:
            rmats_psi_data[field] = int(rmats_psi_dic[RMATS_PSI_MONGO_TABLE_FIELD_DIC[field]])
        except Exception, e:
            raise Exception('')
    
    rmats_psi_data['case'] = 'SAMPLE2'
    rmats_psi_data['src'] = 'jc'
    rmats_psi_data['splicing_id'] = ObjectId("5909707aa4e1af2a6305c78b")
    
    try:
        rmats_psi_collection.insert_one(rmats_psi_data)
        print("导入rmats psi统计表：%s信息成功" % (rmats_psi_file))
    except Exception, e:
        print("导入rmats psi统计表：%s信息出错:%s" % (rmats_psi_file, e))
