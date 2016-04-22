# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
import datetime
import random
from mainapp.libs.signature import check_sig
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.group_stat import GroupStat as G


class TwoSample(object):
    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        my_param = dict()
        my_param['otu_id'] = data.otu_id
        my_param['level_id'] = data.level_id
        my_param['sample1'] = data.sample1
        my_param['sample2'] = data.sample2
        my_param['ci'] = data.ci
        my_param['correction'] = data.correction
        my_param['type'] = data.type
        my_param['test'] = data.test
        my_param['methor'] = data.methor
        my_param['coverage'] = data.coverage
        my_param['submit_location'] = data.submit_location
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if otu_info:
            name = "twosample_stat_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            two_sample_id = G().create_species_difference_check(level=data.level_id, check_type='two_sample',
                                                                params=params, from_otu_table=data.otu_id, name=name)
            task_info = Meta().get_task_info(otu_info["task_id"])
            if task_info:
                member_id = task_info["member_id"]
            else:
                info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                return json.dumps(info)
            update_info = {str(two_sample_id): "sg_species_difference_check"}
            update_info = json.dumps(update_info)

            workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
            json_data = {
                "id": workflow_id,
                "stage_id": 0,
                "name": "meta.report.two_sample",
                "type": "workflow",
                "client": client,
                "project_sn": otu_info["project_sn"],
                "to_file": "meta.export_otu_table_by_level(otu_file)",
                "USE_DB": True,
                "IMPORT_REPORT_DATA": True,
                "UPDATE_STATUS_API": "meta.update_status",
                "IMPORT_REPORT_AFTER_END": True,
                "output": "sanger:rerewrweset/files/%s/%s/%s/report_results/%s/" %
                          (str(member_id), otu_info["project_sn"], otu_info["task_id"], name),
                "options": {
                    "update_info": update_info,
                    "otu_file": data.otu_id,
                    "level": data.level_id,
                    "test": data.test,
                    "correction": data.correction,
                    "ci": data.ci,
                    "type": data.type,
                    "sample1": data.sample1,
                    "sample2": data.sample2,
                    "two_sample_id": str(two_sample_id),
                    "methor": data.methor,
                    "coverage": float(data.coverage)
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
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)

    def get_new_id(self, task_id, otu_id):
        new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['otu_id', 'level_id', 'sample1', 'sample2', 'ci', 'correction',
                       'type', 'test', 'methor', 'coverage', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if int(data.level_id) not in [1, 2, 3, 4, 5, 6, 7, 8, 9]:
            success.append("level_id不在范围内")
        if data.correction not in ["holm", "hochberg", "hommel", "bonferroni", "BH", "BY", "fdr", "none"]:
            success.append("多重检验方法不在范围内")
        if data.type not in ["two.side", "greater", "less"]:
            success.append("检验类型不在范围内")
        if float(data.ci) > 1 or float(data.ci) < 0:
            success.append("显著性水平不在范围内")
        if data.test not in ["chi", "fisher"]:
            success.append("所选的分析检验方法不在范围内")
        if float(data.coverage) not in [0.90, 0.95, 0.98, 0.99, 0.999]:
            success.append('置信区间的置信度不在范围值内')
        if data.methor not in ["DiffBetweenPropAsymptoticCC", "DiffBetweenPropAsymptotic", "NewcombeWilson"]:
            success.append('计算置信区间的方法不在范围值内')
        sample_name = G().get_otu_sample_name(data.otu_id)
        if data.sample1 not in sample_name or data.sample2 not in sample_name:
            success.append('所输入的样本名不在otu表里，请检查样本名')
        return success

    

