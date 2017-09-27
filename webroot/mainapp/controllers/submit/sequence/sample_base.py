# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import web
import json
from mainapp.controllers.core.basic import Basic
import random
import re,os
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.submit.sequence.sample_base import SampleBase as SB
from biocluster.config import Config


PACKAGE_URL = "sample_base"


class SampleBaseAction(object):
    """
    检测序列文件，获取序列中的样本信息; 序列的格式可能是fastq, 也可能是fasta
    """

    def __init__(self):
        super(SampleBaseAction, self).__init__()
    @check_sig
    def POST(self):
        """
        接口参数:
        file_path: 序列文件的路径
        type: 流程类型，值为rna或meta
        format: 序列的格式，值为fasta或者fastq
        """
        data = web.input()
        print data
        params = ["file_info", "format", "client", "type", "member_id"]
        for name in params:
            if not hasattr(data, name):
                info = {"success": False, "info": "参数{}不存在".format(name)}
                return json.dumps(info)
        if data.client not in ["client01", "client03"]:
            info = {"success": False, "info": "未知的client：{}".format(data.client)}
            return json.dumps(info)
        print data.format
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

        if data.format in ["sequence.fasta", "sequence.fastq"] and not os.path.isfile(rel_path):
            info = {"success": False, "info": "文件{}不存在".format(file_info["path"])}
            return json.dumps(info)
        elif data.format in ["sequence.fasta_dir", "sequence.fastq_dir"] and not os.path.isdir(rel_path):
            info = {"success": False, "info": "文件夹{}不存在".format(file_info["path"])}
            return json.dumps(info)
        table_id = SB().add_sg_seq_sample(data.member_id, data.type)
        json_obj = dict()
        if data.type == "rna":  # 对type进行判断
            json_obj["name"] = "sequence.rna_sample"
        elif data.type == "meta":
            json_obj["name"] = "sequence.meta_sample"
        json_obj["id"] = self.get_new_id(table_id)
        json_obj['type'] = "workflow"
        json_obj["IMPORT_REPORT_DATA"] = True
        json_obj["IMPORT_REPORT_AFTER_END"] = False
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

        json_obj["options"]["table_id"] = str(table_id)  # 样本集操作中，需传入sg_test_batch主表id，以命名在本地生成的文件夹
        update_info = json.dumps({str(table_id): "sg_test_batch"})
        json_obj["options"]["update_info"] = update_info
        """
        update_status需要重写
        """
        if data.client == "client01":
            update_api = "meta.update_status"
        elif data.client == "client03":
            update_api = "meta.tupdate_status"
        json_obj["UPDATE_STATUS_API"] = update_api
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
