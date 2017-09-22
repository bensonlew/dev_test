#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = Luo Tong
# last modified 2017.09.13

from pymongo import MongoClient
#import os 
import sys
import argparse
from bson.objectid import ObjectId
import random
import time

if(__name__ == "__main__"):
	parser = argparse.ArgumentParser()
	parser.add_argument("-jobID",type = str)
	parser.add_argument("-model", choices = ["MLE","RF","Ada","KNN"], type = str)
	parser.add_argument("-input", type = str)
	parser.add_argument("-log", default = '/mnt/ilustre/users/sanger-dev/sg-users/luotong/ctMethTracer/01.interface/cmd1.log')
	args = parser.parse_args()
	print args.jobID
	
	_client = MongoClient("10.100.200.131", 27017)
	_db = _client.ctMethTracer
	_job = _db.job
	_jobID = ObjectId(args.jobID)
	#_jobDoc = _job.find_one({'_id':args.jobID})
	if(random.uniform(0,1)):
		_scoreValue = {
			"predicted_type" : "CHOL",
			"score" : {
				"COAD" : {
					"name" : "Colon-adencarcinoma",
					"score" : 0
			},
				"KICH" : {
					"name" : "Kidney-chromophobe",
					"score" : 0
			},
				"DLBC" : {
					"name" : "Lymphoid-neoplasm-diffuse-large-B-cell-lymphoma",
					"score" : 0.63
			},
				"BLCA" : {
					"name" : "Bladder-urothelial-carcinoma",
					"score" : 0
			},
				"ESCA" : {
					"name" : "Esophageal-carcinoma",
					"score" : 0
			},
				"CHOL" : {
					"name" : "Cholangiocarcinoma",
					"score" : 0
			},
				"HNSC" : {
					"name" : "Head-and-neck-squamous-cell-carcinoma",
					"score" : 0
			},
				"GBM" : {
					"name" : "Glioblastoma-multiforme",
					"score" : 0
			},
				"ACC" : {
					"name" : "Adrenocortical-carcinoma",
					"score" : 0.25
			},
				"CESC" : {
					"name" : "Cervical-squamous-cell-carcinoma-and-endocervical-adenocarcinoma",
					"score" : 0
			},
				"BRCA" : {
					"name" : "Breast-invasive-carcinoma",
					"score" : 0.12
			}
			},
			"jobID" : _jobID,
			"tumor_burden" : 0.1
		}
		_statValue = {
			"proportion" : {
				"useless" : 0.21,
				"useful" : 0.79
			},
			"position" : {
				"intron" : 0.11,
				"intergenic_region" : 0.1,
				"TTS" : 0.15,
				"promoter" : 0.16,
				"3'UTR" : 0.14,
				"exon" : 0.13,
				"5'UTR" : 0.14,
				"enhancer" : 0.07
			},
			"jobID" : _jobID
		}
		_stat = _db.pie
		_score = _db.job_details
		_job.update({'_id':_jobID},{'$set':{"step":1,"status":"running"}})
		#time.sleep(30)
		_job.update({'_id':_jobID},{'$set':{"step":2}})
		_stat.insert_one(_statValue)
		#time.sleep(30)
		if(random.uniform(0,1)>0.25):
			_job.update({'_id':_jobID},{'$set':{"step":3}})
			#time.sleep(10)
		_job.update({'_id':_jobID},{'$set':{"step":4}})
		#time.sleep(20)
		_job.update({'_id':_jobID},{'$set':{"step":5}})
		#time.sleep(30)
		_score.insert_one(_scoreValue)
		_job.update({'_id':_jobID},{'$set':{"step":6,"status":"finish"}})
	else:
		_job.update({'_id':_jobID},{'$set':{'status':'error','description':'incorrect format'}})
		raise Exception('incorrect format')
	'''
	print "If you see this message, it means that ctMethTracer.py is running for testing"
	with open(args.log,'w') as f:
		f.write("ctMethTracer.py is running\n")
	'''
