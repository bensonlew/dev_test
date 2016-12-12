# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

import os
from biocluster.config import Config
from pymongo import MongoClient



mongo_client = MongoClient(Config().MONGO_BIO_URI)
database = mongo_client['sanger_paternity_test']

def export_tab_file(sample,dir):
	file = os.path.join(dir, sample + '.tab')
	collection = database['sg_pt_tab_detail']
	search_result = collection.find({"sample_id" : sample})  #读出来是个地址
	with open(file, 'w+') as f:
		for i in search_result:
			f.write(i['sample_id']+'\t'+i['chrom']+'\t'+i['pos']+'\t'
			        +i['ref']+'\t'+i['alt']+'\t'+i['dp']+'\t'
			        +i['ref_dp']+'\t'+i['alt_dp']+'\n')
	return file
