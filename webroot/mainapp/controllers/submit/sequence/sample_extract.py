# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import web
import json
from mainapp.controllers.core.basic import Basic
import random
import re,os
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.submit.sequence.sample_extract import SampleExtract as SE
from biocluster.config import Config



class SampleExtract(object):
    """
    检测序列文件，获取序列中的样本信息，序列个类型可能是文件，也可能是文件夹; 序列的格式可能是fastq, 也可能是fasta
    """
    @check_sig
    def POST(self):
        """
        接口参数:
        file_path: 序列文件的路径
        task_id: 任务id
        type: 文件的类型， 值为file或者dir
        format: 序列的格式，值为fasta或者fastq
        """
        data = web.input()
        print data
        params = ["file_info", "format", "task_id", "client"]
        for name in params:
            if not hasattr(data, name):
                info = {"success": False, "info": "参数{}不存在".format(name)}
                return json.dumps(info)
        if data.client not in ["client01", "client03"]:
            info = {"success": False, "info": "未知的client：{}".format(data.client)}
        if data.format not in ["sequence.fasta", "sequence.fastq", "sequence.fasta_dir", "sequence.fastq_dir"]:
            info = {"success": False, "info": "参数format的值不正确"}
            return json.dumps(info)
        if data.client == "client01":
            pre_path = "sanger:"
            type_name = "sanger"
        elif data.client == "client03":
            pre_path = "tsanger:"
            type_name = "tsanger"
        file_info = json.loads(data.file_info)
        suff_path = re.split(":", file_info["path"])[1]
        config = Config().get_netdata_config(type_name)
        rel_path = os.path.join(config[type_name + "_path"], suff_path)
        print rel_path

        if data.format in ["sequence.fasta", "sequence.fastq"] and not os.path.isfile(rel_path):
            info = {"success": False, "info": "文件{}不存在".format(file_info["path"])}
            return json.dumps(info)
        elif data.format in ["sequence.fasta_dir", "sequence.fastq_dir"] and not os.path.isdir(rel_path):
            info = {"success": False, "info": "文件夹{}不存在".format(file_info["path"])}
            return json.dumps(info)
        my_params = dict()
        my_params["task_id"] = data.task_id
        my_params["file_info"] = json.loads(data.file_info)
        my_params["format"] = data.format
        my_params["query_id"] = data.query_id
        params = json.dumps(my_params, sort_keys=True, separators=(',', ':'))
        table_id = SE().add_sg_seq_sample(data.task_id, data.file_info, params, data.query_id)

        json_obj = dict()
        json_obj["name"] = "sequence.sample_extract"
        json_obj["id"] = self.get_new_id(data.task_id)
        json_obj['type'] = "workflow"
        json_obj["IMPORT_REPORT_DATA"] = True
        json_obj["IMPORT_REPORT_AFTER_END"] = True
        json_obj["USE_DB"] = True
        json_obj['client'] = data.client
        json_obj["options"] = dict()
        # 给json文件的options字段指定输入的序列，这个option的字段可能是in_fastq或者是in_fasta
        if file_info["file_list"] in [None, "none", "None", "null", 'Null', '[]', '', []]:
            if data.format in ["sequence.fasta_dir", "sequence.fasta"]:
                json_obj["options"]["in_fasta"] = "{}||{}/{}".format(data.format, pre_path, suff_path)
            elif data.format in ["sequence.fastq_dir", "sequence.fastq"]:
                json_obj["options"]["in_fastq"] = "{}||{}/{}".format(data.format, pre_path, suff_path)
        else:
            if data.format in ["sequence.fasta_dir", "sequence.fasta"]:
                json_obj["options"]["in_fasta"] = "{}||{}/{};;{}".format(data.format, pre_path, suff_path, file_info["file_list"])
            elif data.format in ["sequence.fastq_dir", "sequence.fastq"]:
                json_obj["options"]["in_fastq"] = "{}||{}/{};;{}".format(data.format, pre_path, suff_path, json.dumps(file_info["file_list"]))

        json_obj["options"]["table_id"] = str(table_id)
        update_info = json.dumps({str(table_id): "sg_seq_sample"})
        json_obj["options"]["update_info"] = update_info
        if data.client == "client01":
            update_api = "meta.update_status"
        elif data.client == "client03":
            update_api = "meta.tupdate_status"
        json_obj["UPDATE_STATUS_API"] = update_api
        """
        workflow_module = Workflow()

        workflow_module.add_record(insert_data)
        info = {"success": True, "info": "样本提取接口投递成功，正在计算中..."}
        return json.dumps(info)
        """
        workflow_client = Basic(data=json_obj, instant= False)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return json.dumps(run_info)
        except Exception, e:
            return json.dumps({"success": False, "info": "运行出错: %s" % e })

    def get_new_id(self, task_id):
        new_id = "{}_{}_{}".format(task_id, random.randint(1, 10000), random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id)
        return new_id
