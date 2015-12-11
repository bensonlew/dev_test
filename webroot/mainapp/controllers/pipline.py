# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig, CreateSignature
from web import form
from mainapp.libs.json_check import check_json
import json
from mainapp.models.workflow import Workflow
import os
from mainapp.libs.jsonencode import CJsonEncoder
import xml.etree.ElementTree as ET


class Pipline(object):

    def GET(self):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../views/'))
        render = web.template.render(path)
        return render.pipline(self.get_form())

    @check_sig
    @check_json
    def POST(self):
        data = web.input()
        json_obj = json.loads(data.json)
        if "type" not in json_obj.keys() or "id" not in json_obj.keys():
            info = {"success": False, "info": "Json内容不正确!!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(json_obj['id'])
        if len(workflow_data) > 0:
            # print workflow_data[0]
            info = {"success": False, "info": "流程ID重复!"}
            return json.dumps(info)
        else:
            insert_data = {"client": data.client,
                           "workflow_id": json_obj['id'],
                           "json": data.json,
                           "ip": web.ctx.ip
                           }
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": "添加队列成功!"}
            return json.dumps(info)

    def sanger_submit(self):
        data = web.input()


    @staticmethod
    def get_form():
        sig_obj = CreateSignature("test")
        return form.Form(
            form.Hidden(name='client', value=sig_obj.client),
            form.Hidden(name='nonce', value=sig_obj.nonce),
            form.Hidden(name='timestamp', value=sig_obj.timestamp),
            form.Hidden(name='signature', value=sig_obj.signature),
            form.Textarea("json", description="Json", rows="20", cols="100"),
            form.Button("submit", type="submit", description="提交")
        )


class PiplineState(object):

    @check_sig
    def GET(self):
        data = web.input()
        if not hasattr(data, "id") or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if data.client == record.client:
                if record.has_run == 0:
                    info = {
                        "success": True,
                        "state": "queue",
                        "addtime": record.add_time
                    }
                    return json.dumps(info, cls=CJsonEncoder)
                elif record.is_error == 1:
                    info = {
                        "success": True,
                        "state": "error",
                        "error": record.error,
                        "addtime": record.add_time,
                        "starttime": record.run_time,
                        "endtime": record.end_time
                    }
                    return json.dumps(info, cls=CJsonEncoder)
                elif record.is_end == 1:
                    info = {
                        "success": True,
                        "state": "end",
                        "addtime": record.add_time,
                        "starttime": record.run_time,
                        "endtime": record.end_time,
                        "output": record.output,
                    }
                    return json.dumps(info, cls=CJsonEncoder)
                elif workflow_module.last_update_seconds(data.id) > 100:
                    info = {
                        "success": True,
                        "state": "offline",
                        "addtime": record.add_time,
                        "starttime": record.run_time,
                        "lastupdate": record.last_update
                    }
                    return json.dumps(info, cls=CJsonEncoder)
                else:
                    info = {
                        "success": True,
                        "state": "running",
                        "addtime": record.add_time,
                        "starttime": record.run_time,
                        "lastupdate": record.last_update
                    }
                    return json.dumps(info, cls=CJsonEncoder)
            else:
                info = {"success": False, "info": "没有权限查看！"}
                return json.dumps(info)
        else:
            info = {"success": False, "info": "流程ID不存在！"}
            return json.dumps(info)


class PiplineLog(object):

    @check_sig
    def GET(self):
        data = web.input()
        if not hasattr(data, "id") or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if data.client == record.client:
                if record.workdir is None or record.workdir == "":
                    info = {"success": False, "info": "还没有日志!"}
                    return json.dumps(info)
                log_file = os.path.join(record.workdir, "log.txt")
                if record.has_run == 0 or not os.path.exists(log_file):
                    info = {"success": False, "info": "还没有日志!"}
                    return json.dumps(info)
                else:
                    with open(log_file, "r") as f:
                        logs = f.readlines()
                    return "<br>".join(logs)
            else:
                info = {"success": False, "info": "没有权限查看！"}
                return json.dumps(info)
        else:
            info = {"success": False, "info": "流程ID不存在！"}
            return json.dumps(info)


class PiplineStop(object):

    @check_sig
    def POST(self):
        data = web.input()
        if not (hasattr(data, "id") and hasattr(data, "reason")) or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if data.client == record.client:
                if record.has_run == 1 and record.is_end:
                    info = {"success": False, "info": "流程已经结束！"}
                    return json.dumps(info)
                else:
                    insert_data = {"client": data.client,
                                   "ip": web.ctx.ip,
                                   "reson": data.reason
                                   }
                    if workflow_module.set_stop(data.id, insert_data):
                        info = {"success": True, "info": "操作成功！"}
                        return json.dumps(info)
                    else:
                        info = {"success": False, "info": "内部错误！"}
                        return json.dumps(info)
            else:
                info = {"success": False, "info": "没有权限查看！"}
                return json.dumps(info)
        else:
            info = {"success": False, "info": "流程ID不存在！"}
            return json.dumps(info)


class PiplineRunning(object):

    @check_sig
    def GET(self):
        workflow_module = Workflow()
        data = workflow_module.get_running()
        count = len(data)
        if count > 0:
            info = []
            for rec in data:
                info["id"] = rec.workflow_id
                info["addtime"] = rec.add_time
                info["runtime"] = rec.run_time
            info = {"success": True, "count": count, "info": info}
            return json.dumps(info)
        else:
            info = {"success": True, "count": 0, "info": "没有正在运行的流程！"}
            return json.dumps(info)


class PiplineQueue(object):
    @check_sig
    def GET(self):
        workflow_module = Workflow()
        data = workflow_module.get_queue()
        count = len(data)
        if count > 0:
            info = []
            for rec in data:
                info["id"] = rec.workflow_id
                info["addtime"] = rec.add_time
            info = {"success": True, "count": count, "info": info}
            return json.dumps(info)
        else:
            info = {"success": True, "count": 0, "info": "没有正在排队的流程！"}
            return json.dumps(info)
