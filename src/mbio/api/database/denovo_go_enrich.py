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
import gridfs
from cStringIO import StringIO
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class DenovoGoEnrich(Base):
    def __init__(self, bind_object):
        super(DenovoGoEnrich, self).__init__()
        self._db_name = Config().MONGODB + '_rna'
    
    @report_check
    def add_go_enrich(self, name=None, params=None, go_graph_dir=None, go_enrich_dir=None):
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'go_enrich' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'params': params,
            'status': 'end',
            'desc': 'go富集分析主表',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        }
        collection = self._db_name['sg_denovo_go_enrich']
        go_enrich_id = collection.insert_one(insert_data).inserted_id 
        fs = gridfs.GridFS(self._db_name)
        gra = fs.put(open(go_graph_dir, 'rb'))
        try:
            collection.update({"_id": ObjectId(go_enrich_id)}, {"$set": {'go_directed_graph': gra}})
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错：%s" % (go_graph_dir, e))
        else:
            self.bind_object.logger.info("导入%s信息成功！" % (go_graph_dir))
        if os.path.exists(go_enrich_dir):
            self.add_go_enrich_detail(go_enrich_id, go_enrich_dir)  
        print "add sg_denovo_go_enrich sucess!"
        return go_enrich_id
    
    @report_check
    def add_go_enrich_detail(self, go_enrich_id, go_enrich_dir):
        if not isinstance(go_enrich_id,ObjectId):
            if isinstance(go_enrich_id, types.StringTypes):
                go_enrich_id = ObjectId(go_enrich_id)
            else:
                raise Exception('go_enrich_id须为ObjectId对象或其他对应的字符串！')
        if not os.path.exists(go_enrich_dir):
            raise Exception('{}所指定的路径不存在，请检查！'.format(go_enrich_dir))
        data_list = []
        with open(go_enrich_dir, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                line = line.strip().split('\t')
                m = re.match(r"(.+)/(.+)", line[5])
                pop_count = int(m.group(1))
                line[6] = float(line[6]) 
                line[7] = int(line[7])
                line[8] = int(line[8])
                line[9] = float(line[9])
                line[10] = float(line[10])
                line[11] = float(line[11])
                line[12] = float(line[12])
                data = [
                    ('go_enrich_id', go_enrich_id),
                    ('go_id', line[0]),
                    ('go_type', line[1]),
                    ('enrichment', line[2]),
                    ('discription', line[3]),
                    ('ratio_in_study', line[4]),
                    ('ratio_in_pop', line[5]),
                    ('p_uncorrected', line[6]),
                    ('depth', line[7]),
                    ('study_count', line[8]),
                    ('pop_count', pop_count),
                    ('p_bonferroni', line[9]), 
                    ('p_sidak', line[10]),
                    ('p_holm', line[11]),
                    ('p_fdr', line[12]),
                    ('diff_genes', line[13]),
                ]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self._db_name['sg_denovo_go_enrich_detail']
                collection.insert_many(data_list)
            except:
                print "add sg_denovo_go_enrich_detail failure!"
            else:
                print "add sg_denovo_go_enrich_detail sucess!"
    
    @report_check
    def add_go_regulate(self, name=None, params=None, go_regulate_dir=None):
        project_sn = self.bind_object.sheet.project_sn
        task_id = self.bind_object.sheet.id
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'name': name if name else 'go_enrich' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'params': params,
            'status': 'end',
            'desc': 'go调控分析主表',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        }
        collection = self._db_name['sg_denovo_go_regulate']
        go_regulate_id = collection.insert_one(insert_data).inserted_id   
        if os.path.exists(go_regulate_dir):
            self.add_go_regulate_detail(go_regulate_id, go_regulate_dir) 
        return go_regulate_id
        
   @report_check    
    def add_go_regulate_detail(self, go_regulate_id, go_regulate_dir):
        if not isinstance(go_regulate_id,ObjectId):
            if isinstance(go_regulate_id, types.StringTypes):
                go_regulate_id = ObjectId(go_regulate_id)
            else:
                raise Exception('go_enrich_id须为ObjectId对象或其他对应的字符串！')
        if not os.path.exists(go_regulate_dir):
            raise Exception('{}所指定的路径不存在，请检查！'.format(go_regulate_dir))
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
                    ('go_regulate_id', go_regulate_id), 
                    ('go_type', line[0]),
                    ('go', line[1]), 
                    ('go_id', line[2]),
                    ('up_num', line[3]),
                    ('up_percent', line[4]),
                    ('down_num', line[5]),
                    ('down_percent', line[6]),
                ]
                try:
                    data += [('up_genes', line[7])]
                except:
                    data += [('up_genes', '')]
                try:
                    data += [('down_genes', line[8])]
                except:
                    data += [('down_genes', '')]
                data = SON(data)
                data_list.append(data)
            try:
                collection = self._db_name['sg_denovo_go_regulate_detail']
                collection.insert_many(data_list)
            except:
                print "add sg_denovo_go_regulate_detail failure!"
            else:
                print "add sg_denovo_go_regulate_detail sucess!"