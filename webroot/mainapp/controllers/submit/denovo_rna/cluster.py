# -*- coding: utf-8 -*-
# __author__ = 'chenyanyan'
# 2016.10.11
import web
import json
from mainapp.libs.signature import check_sig
from bson.objectid import ObjectId
from mainapp.libs.param_pack import *
from biocluster.config import Config
from mbio.api.database.denovo_express import *
from mainapp.models.mongo.submit.denovo_rna.denovo_cluster import DenovoExpress  # denovo_cluster里面有一个DenovoExpress类，返回express_id
import types
from mainapp.models.mongo.denovo import Denovo
from mainapp.models.workflow import Workflow
# controller


class Cluster(object):
    def __init__(self):
        self.db_name = Config().MONGODB + '_rna'

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param["submit_location"] = data.submit_location
        my_param["express_id"] = data.express_id
        my_param["distance_method"] = data.distance_method
        my_param["cluster_method"] = data.cluster_method
        my_param["log"] = data.log
        my_param["sub_num"] = data.sub_num
        my_param['gene_list'] = data.gene_list
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        express_info = Denovo().get_main_info(data.express_id, 'sg_denovo_express')
        # Denovo().get_main_info(main_id, collection_name)
        if express_info:
            task_id = express_info["task_id"]
            project_sn = express_info["project_sn"]
            task_info = Denovo().get_task_info(task_id)
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个express_id对应的表达量矩阵对应的task：{}没有member_id!".format(express_info["task_id"])}
                return json.dumps(info)
            cluster_id = DenovoExpress().add_cluster(params=params, project_sn=project_sn, task_id=task_id)
            update_info = {str(cluster_id): 'sg_denovo_cluster', 'database': self.db_name}
            update_info = json.dumps(update_info)
            workflow_id = self.get_new_id(task_id, data.express_id)
            (output_dir, update_api) = GetUploadInfo_denovo(client, member_id, project_sn, task_id, 'cluster_stat')
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "denovo_rna.report.cluster",  # 该路径下的cluster.py文件，是cluster对应的workflow的文件，获取workfow信息
                "type": "workflow",
                "client": client,
                "project_sn": project_sn,
                "to_file": ["denovo.export_express_matrix(express_file)"],
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": update_api,
                "IMPORT_REPORT_AFTER_END": True,
                "output": output_dir,
                "options":{
                    "express_file": data.express_id,
                    "update_info": update_info,
                    "cluster_id": str(cluster_id),
                    "distance_method": data.distance_method,
                    "sub_num": data.sub_num,
                    "cluster_method": data.cluster_method,
                    "log": data.log,
                    "gene_list": data.gene_list
                }
            }
            insert_data = {"client": client,
                           "workflow_id": workflow_id,
                           "json": json.dumps(json_data),
                           "ip": web.ctx.ip
                           }
            workflow_module = Workflow()
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "提交成功!"}
            return json.dumps(info)
        else:
            info = {"success": False, "info": "express_id不存在，请检查参数是否正确！"}
            return json.dumps(info)

    def get_new_id(self, task_id, main_id):
        new_id = "%s_%s_%s" % (task_id, main_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, main_id)
        return new_id

    def check_options(self, data):
        """
        检查网页端传来的参数是否正确
        """
        params_name = ["submit_location", "distance_method", "cluster_method", "log", "sub_num", "express_id"]
        success = []
        for names in params_name:
            if not hasattr(data, names):
                success.append("缺少参数！")
        express_id = str(data.express_id)
        if not isinstance(express_id, ObjectId) and not isinstance(express_id, types.StringType):
            success.append("传入的express_id {}不是一个ObjectId对象或字符串类型".format(express_id))
        return success
