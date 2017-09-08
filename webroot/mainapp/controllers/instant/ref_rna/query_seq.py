# -*- coding: utf-8 -*-
import os
import web
import json
from mainapp.controllers.project.ref_rna_controller import RefRnaController
import sqlite3
__author__ = 'gdq'


class QuerySeqAction(RefRnaController):
    def __init__(self):
        super(QuerySeqAction, self).__init__(instant=True)

    def POST(self):
        data = web.input()
        # check
        for arg in ['seq_id', 'seq_db', 'seq_type', 'task_id']:
            if not hasattr(data, arg):
                info = {'success': False, 'info': 'Argument:{} required'.format(arg)}
                return json.dumps(info)

        if data['seq_type'] not in ['gene', 'transcript', 'cds', 'pep']:
            info = {'success': False, 'info': 'unexpected seq_type'}
            return json.dumps(info)

        if not os.path.exists(data['seq_db']):
            info = {'success': False, 'info': data['seq_db'] + ' not exist'}
            return json.dumps(info)

        # query
        seq_id, sequence = self.query_seq(data.seq_type, data.seq_id, data.seq_db)

        # save
        mongo_data = [('task_id', data.task_id),
                      ('seq_id', seq_id),
                      ('sequence', sequence), ]
        self.ref_rna.insert_seq(mongo_data)
        info = {"success": True, "info": "提交成功"}
        return json.dumps(info)

    @staticmethod
    def query_seq(seq_type, seq_id, seq_db):
        conn = sqlite3.connect(seq_db)
        cursor = conn.cursor()
        table_name = seq_type
        seq_id = seq_id
        cursor.execute("SELECT * FROM {} WHERE seq_id='{}'".format(table_name, seq_id))
        seq_id, sequence = cursor.fetchone()
        return seq_id, sequence
