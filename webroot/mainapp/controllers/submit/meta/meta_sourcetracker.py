# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import web
import json
import datetime
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.models.mongo.meta import Meta
from mainapp.libs.param_pack import param_pack, group_detail_sort
from bson import ObjectId


class MetaSourcetracker(MetaController):
    def __init__(self):
        super(MetaSourcetracker, self).__init__(instant=False)

    def POST(self):
        data = web.input()
        params_name = ['otu_id', 'level_id', 'submit_location', 'group_detail', 'group_id', 'second_group_detail',
                       'second_group_id', 's', 'source', 'sink']
        for param in params_name:
            if not hasattr(data, param):
                info = {"success": False, "info": "缺少%s参数!" % param}
                return json.dumps(info)
        if int(data.level_id) not in range(1, 10):
            info = {"success": False, "info": "level{}不在规定范围内{}".format(data.level_id)}
            return json.dumps(info)
        group_detail = json.loads(data.group_detail)
        if not isinstance(group_detail, dict):
            success.append("传入的group_detail不是一个字典")
        second_group_detail = json.loads(data.second_group_detail)
        if not isinstance(second_group_detail, dict):
            success.append("传入的second_group_detail不是一个字典")
        first = 0
        second = 0
        for i in group_detail.values():
            first += len(i)
        for n in second_group_detail.values():
            second += len(n)
        if first != second:
            success.append("二级分组与一级分组的样本数不相同，请检查！")
        otu_info = Meta().get_otu_table_info(data.otu_id)
        if not otu_info:
            info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
            return json.dumps(info)
        task_name = 'meta.report.meta_sourcetracker'
        task_type = 'workflow'
        task_info = Meta().get_task_info(otu_info['task_id'])
        main_table_name = 'MetaSourcetracker_' + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        params_json = {
            'otu_id': data.otu_id,
            'level_id': int(data.level_id),
            'group_id': data.group_id,
            'group_detail': group_detail_sort(data.group_detail),
            'second_group_detail': group_detail_sort(data.second_group_detail),
            'second_group_id': data.second_group_id,
            'source': data.source,
            's': data.s,
            'sink': data.sink,
            'submit_location': data.submit_location,
            'task_type': 'reportTask'
        }
        mongo_data = [
            ('project_sn', task_info['project_sn']),
            ('task_id', task_info['task_id']),
            ('otu_id', ObjectId(data.otu_id)),  # maybe data.otu_id
            ('name', main_table_name),
            ("params", json.dumps(params_json, sort_keys=True, separators=(',', ':'))),
            ('status', 'start'),
            ('desc', 'meta_sourcetracker分析'),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        meta = Meta()
        main_table_id = meta.insert_main_table('sg_sourcetracker', mongo_data)
        update_info = {str(main_table_id): 'sg_sourcetracker'}
        options = {
            "in_otu_table": data.otu_id,
            "map_detail": data.group_id,
            "update_info": update_info,
            "s": data.s,
            "level": int(data.level_id),
            "sink": data.sink,
            "source": data.source,
            "meta_sourcetracker_id": str(main_table_id),
        }
        to_file = ["meta.export_otu_table_by_detail(otu_file)", "meta.export_cascading_table_by_detail(group_file)"]
        self.set_sheet_data(name=task_name, options=options, main_table_name=main_table_name,
                            module_type=task_type, to_file=to_file)
        task_info = super(MetaSourcetracker, self).POST()
        task_info['content'] = {
            'ids': {
                'id': str(main_table_id),
                'name': main_table_name
            }}
        return json.dumps(task_info)

    #     my_param = dict()
    #     my_param['otu_id'] = data.otu_id
    #     my_param['level_id'] = int(data.level_id)
    #     my_param['group_detail'] = group_detail_sort(data.group_detail)
    #     my_param['submit_location'] = data.submit_location
    #     my_param['second_group_detail'] = group_detail_sort(data.second_group_detail)
    #     my_param['second_group_id'] = data.second_group_id
    #     my_param['group_id'] = data.group_id
    #     my_param['s'] = data.s
    #     params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
    #     otu_info = Meta().get_otu_table_info(data.otu_id)
    #     if otu_info:
    #         name = "meta_sourcetracker_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    #         task_info = Meta().get_task_info(otu_info["task_id"])
    #         if task_info:
    #             member_id = task_info["member_id"]
    #         else:
    #             info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
    #             return json.dumps(info)
    #         meta_sourcetracker_id = G().create_meta_sourcetracker(params=params, group_id_1=data.group_id, group_id_2=data.second_group_id, from_otu_table=data.otu_id,name=name, level_id=data.level_id)
    #         update_info = {str(meta_sourcetracker_id): "sg_meta_sourcetracker"}  # waiting test and modify
    #         update_info = json.dumps(update_info)
    #         workflow_id = self.get_new_id(otu_info["task_id"], data.otu_id)
    #         (output_dir, update_api) = GetUploadInfo(client, member_id, otu_info['project_sn'], otu_info['task_id'], 'meta_sourcetracker')
    #         json_data = {
    #             "id": workflow_id,
    #             "stage_id": 0,
    #             "name": "meta.report.meta_sourcetracker",
    #             "type": "workflow",
    #             "client": client,
    #             "project_sn": otu_info["project_sn"],
    #             "to_file": ["meta.export_otu_table_by_detail(otu_table)",
    #                         "meta.export_cascading_table_by_detail(group_file)"],
    #             "USE_DB": True,
    #             "IMPORT_REPORT_DATA": True,
    #             "UPDATE_STATUS_API": update_api,
    #             "IMPORT_REPORT_AFTER_END": True,
    #             "output": output_dir,
    #             "options": {
    #                 "in_otu_table": data.otu_id,
    #                 "map_detail": data.group_id,
    #                 "update_info": update_info,
    #                 "s": data.s,
    #                 "level": int(data.level_id),
    #                 "meta_sourcetracker_id": str(meta_sourcetracker_id),
    #             }
    #         }
    #         insert_data = {"client": client,
    #                        "workflow_id": workflow_id,
    #                        "json": json.dumps(json_data),
    #                        "ip": web.ctx.ip
    #                        }
    #         workflow_module = Workflow()
    #         workflow_module.add_record(insert_data)
    #         info = {"success": True, "info": "提交成功!"}
    #         return json.dumps(info)
    #     else:
    #         info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
    #         return json.dumps(info)
    #
    # def get_new_id(self, task_id, otu_id):
    #     new_id = "%s_%s_%s" % (task_id, otu_id[-4:], random.randint(1, 10000))
    #     workflow_module = Workflow()
    #     workflow_data = workflow_module.get_by_workflow_id(new_id)
    #     if len(workflow_data) > 0:
    #         return self.get_new_id(task_id, otu_id)
    #     return new_id

