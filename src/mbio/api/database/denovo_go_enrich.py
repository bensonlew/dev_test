# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# last_modify:20161118
import pymongo
import os
import re
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
import bson.binary
from cStringIO import StringIO
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class DenovoGoEnrich(Base):
    def __init__(self, bind_object):
        super(DenovoGoEnrich, self).__init__()
        self._db_name = Config().MONGODB + '_rna'
    
    @report_check
    def add_go_enrich(self, name=None, params=None, go_graph_dir=None, go_enrich_dir=None, go_regulate_dir=None):
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'go_enrich' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'params': params,
            'status': 'end',
            'desc': 'go������������',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'go_directed_graph': '',
        }
        collection = self._db_name['sg_denovo_go_enrich']
        go_enrich_id = collection.insert_one(insert_data).inserted_id 
        if os.path.exists(go_enrich_dir):
            self.add_go_enrich_stat(go_enrich_id, go_enrich_dir)
            self.add_go_enrich_bar(go_enrich_id, go_enrich_dir)
        if os.path.exists(go_regulate_dir):
            self.add_go_regulate_graph(go_enrich_id, go_regulate_dir)
        print "add sg_denovo_go_enrich sucess!"
        return go_enrich_id
    
    @report_check
    def add_go_enrich_stat(self, go_enrich_id, go_enrich_dir):
        if not isinstance(go_enrich_id,ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id��ΪObjectId�����������Ӧ���ַ�����')
        if not os.path.exists(go_enrich_dir):
            raise Exception('{}��ָ����·�������ڣ����飡'.format(go_enrich_dir))
        data_list = []
        with open(go_enrich_dir, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                line[6] = float(line[6])
                line[9] = float(line[9])
                data = [
                    ('go_enrich_id', go_enrich_id),
                    ('go_id', line[0]),
                    ('enrich', line[2]),
                    ('discription', line[3]),
                    ('ratio_in_study', line[4]),
                    ('ratio_in_pop', line[5]),
                    ('pvalue_uncorrected', line[6]),
                    ('pvalue_bonferroni', line[9]),
                    ('type', line[1]),
                    ('diff_genes', line[13]),
                ]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self._db_name['sg_denovo_go_enrich_stat']
                collection.insert_many(data_list)
            except:
                print "add sg_denovo_go_enrich_stat failure!"
            else:
                print "add sg_denovo_go_enrich_stat sucess!"
                   
    @report_check
    def add_go_enrich_bar(self, go_enrich_id, go_enrich_dir):
        if not isinstance(go_enrich_id,ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id��ΪObjectId�����������Ӧ���ַ�����')
        if not os.path.exists(go_enrich_dir):
            raise Exception('{}��ָ����·�������ڣ����飡'.format(go_enrich_dir))
        data_list = []
        with open(go_enrich_dir, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                m = re.match(r"(.+)/(.+)", line[5])
                pop_count = int(m.group(1))
                line[8] = int(line[8])
                line[12] = float(line[12])
                data = [
                    ('go_enrich_id', go_enrich_id),
                    ('go_name', line[3]),
                    ('go_type', line[1]),
                    ('study_count', line[8]),
                    ('pop_count', pop_count),
                    ('p_fdr', line[12]),
                ]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self._db_name['sg_denovo_go_enrich_bar']
                collection.insert_many(data_list)
            except:
                print "add sg_denovo_go_enrich_bar failure!"
            else:
                print "add sg_denovo_go_enrich_bar sucess!"
    
    @report_check    
    def add_go_regulate_graph(self, go_enrich_id, go_regulate_dir):
        if not isinstance(go_enrich_id,ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id��ΪObjectId�����������Ӧ���ַ�����')
        if not os.path.exists(go_regulate_dir):
            raise Exception('{}��ָ����·�������ڣ����飡'.format(go_regulate_dir))
        data_list = []
        with open(go_regulate_dir, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                line[3] = int(line[3])
                line[4] = float(line[4])
                line[5] = int(line[5])
                line[6] = float(line[6])
                data = [
                    ('go_enrich_id', go_enrich_id), 
                    ('go_type', line[0]),
                    ('go', line[1]), 
                    ('up_num', line[3]),
                    ('up_percent', line[4]),
                    ('down_num', line[5]),
                    ('down_percent', line[6]),
                ]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self._db_name['sg_denovo_go_regulate_graph']
                collection.insert_many(data_list)
            except:
                print "add sg_denovo_go_regulate_graph failure!"
            else:
                print "add sg_denovo_go_regulate_graph sucess!"
 
""" 
if __name__ == '__main__':
    go = DenovoGoEnrich()
    go_enrich_dir = 'go_enrich_E20_vs_P1.DE.xls'
    go_regulate_dir = 'GO_regulate.xls'
    name = ''
    params = None
    go_graph_dir = ''
    go_enrich_id = go.add_go_enrich(name, params, go_graph_dir, go_enrich_dir, go_regulate_dir)
"""