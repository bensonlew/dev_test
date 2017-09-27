# -*- coding: utf-8 -*-
# __author__ = 'luotong'
import web
import json
from mainapp.libs.signature import check_sig
from mainapp.controllers.core.basic import Basic
from biocluster.core.function import filter_error_info
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.ref_rna import RefRna
from biocluster.config import Config
from biocluster.wpm.client import *
import random
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId


PACKAGE_URL = "meth_tracer"
class MethTracerAction(object):
    """
    MethTracer设置
    """
    def __init__(self):
        super(MethTracerAction, self).__init__()

    @check_sig
    def POST(self):
        data = web.input()

        data['task_id'] = "meth_tracer" + str(random.randint(1000, 10000))
        # requires = ['path', 'model']
        # for i in requires:
            # if not (hasattr(data, i)):
                # return json.dumps({"success": False, "info": "缺少%s参数!" % i})

        # if data['task_id'] == "" or data['task_id'] == " ":
             # return json.dumps({"success": False, "info": "参数task_id不能为空!"})
        workflow_id = "MethTracer_" + "{}_{}".format(data['task_id'], datetime.datetime.now().strftime("%H%M%S%f")[:-3])
        #_client = MongoClient('10.100.200.131',27017)
        _client = MongoClient('192.168.10.186',27017)

        _db = _client.ctMethTracer
        _job = _db.job
        _posts = {
                  'step':0,
                  'status':'test',
                  'description':None,
                  'params':{
                        'input':data['path'],
                        'model':data['model'],
                  },
                  'email':data['email']
              }
        _jobID = _job.insert_one(_posts).inserted_id
        print "##" + str(_jobID)
        _job.update({'_id':_jobID},{"$set":{'jobID':str(_jobID)}})
        
        json_data = {
          'id': workflow_id,
          'stat_id': 0,
          # 'type': 'workflow',
          'type': "tool",
          # 'name': "copy_demo.demo_init",  # 需要配置
          'name': "meth_tracer",
          'client': data.client,
          "IMPORT_REPORT_DATA": False,
          "IMPORT_REPORT_AFTER_END": False,
          'options': {
              'path': data['path'],
              'model': data['model'],
              'jobID': str(_jobID),
              #'email': data['email'],
          }
        }
        
        # try:
        #    worker = worker_client()
        #    info = worker.add_task(data)
        try:
            workflow_client = Basic(data=json_data, instant=False)
            info = workflow_client.run()
            info['info'] = filter_error_info(info['info'])

            if "success" in info.keys() and info["success"]:
                res = {"success": True, "info": "MethTracer任务提交成功,请两个小时后进行此demo的拉取或取消MethTracer设置操作"}
                res['jobid'] = str(_jobID)
                return json.dumps(res)
            else:
                return {"success": False, "info": "MethTracer任务提交失败,请重新设置"}
        except:
            return {"success": False, "info": "MethTracer任务提交失败,请重新设置"}

