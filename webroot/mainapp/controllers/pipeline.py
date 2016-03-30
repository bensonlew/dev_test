# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig, CreateSignature
from web import form
from mainapp.libs.input_check import check_format
import json
from mainapp.models.workflow import Workflow
import os
from mainapp.libs.jsonencode import CJsonEncoder
import xml.etree.ElementTree as ET
from mainapp.config.db import get_use_api_clients, get_api_type


class Pipeline(object):

    def GET(self):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../views/'))
        render = web.template.render(path)
        return render.pipline(self.get_form())

    @check_sig
    @check_format
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == "client01":
            json_obj = self.sanger_submit()
            json_obj["IMPORT_REPORT_DATA"] = True   # 更新报告数据
            json_obj["IMPORT_REPORT_AFTER_END"] = True
        else:
            json_obj = self.json_submit()
        json_obj["USE_DB"] = True   # 使用数据库
        if client in get_use_api_clients():
            api = get_api_type(client)
            if api:
                json_obj["UPDATE_STATUS_API"] = api
        json_obj['client'] = client
        print json_obj
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
            insert_data = {"client": client,
                           "workflow_id": json_obj['id'],
                           "json": json.dumps(json_obj),
                           "ip": web.ctx.ip
                           }
            workflow_module.add_record(insert_data)
            # return json.dumps(json_obj)
            info = {"success": True, "info": "添加队列成功!"}
            return json.dumps(info)

    @staticmethod
    def sanger_submit():
        data = web.input()
        xml_data = "".join(data.content)
        root = ET.fromstring(xml_data)
        json_obj = {}
        file_path = "sanger:"
        for child_of_root in root:
            if child_of_root.tag == "project_sn":
                json_obj['project_sn'] = child_of_root.text
            if child_of_root.tag == "name":
                json_obj['name'] = child_of_root.text
            if child_of_root.tag == "task_id":
                json_obj['id'] = child_of_root.text
            if child_of_root.tag == "bucket":
                file_path += child_of_root.text
        first_stage = root.find("stage")
        json_obj['stage_id'] = first_stage.find("id").text
        if not json_obj['stage_id']:
            json_obj['stage_id'] = 0
        json_obj['type'] = first_stage.find("type").text
        json_obj['name'] = first_stage.find("name").text
        json_obj['output'] = "%s/%s/%s/%s" % (file_path, json_obj['project_sn'],
                                              json_obj['id'], json_obj['stage_id'])
        option = first_stage.find("parameters")
        json_obj['options'] = {}
        for opt in option:
            if 'type' in opt.attrib.keys():
                if opt.attrib['type'] == "sanger":
                    if "format" in opt.attrib.keys():
                        json_obj['options'][opt.tag] = "%s||%s/%s" % (opt.attrib["format"], file_path, opt.text)
                    else:
                        json_obj['options'][opt.tag] = "%s/%s" % (file_path, opt.text)
                else:
                    if "format" in opt.attrib.keys():
                        json_obj['options'][opt.tag] = "%s||%s:%s" %\
                                                       (opt.attrib["format"], opt.attrib["type"], opt.text)
                    else:
                        json_obj['options'][opt.tag] = "%s:%s" % (opt.attrib, opt.text)
            else:
                json_obj['options'][opt.tag] = opt.text
        return json_obj

    @staticmethod
    def json_submit():
        data = web.input()
        json_obj = json.loads(data.json)
        return json_obj

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


class PipelineState(object):

    @check_sig
    def GET(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "id") or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if client == record.client:
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
                elif workflow_module.last_update_seconds(data.id) > 200:
                    info = {
                        "success": True,
                        "state": "offline",
                        "addtime": record.add_time,
                        "starttime": record.run_time,
                        "lastupdate": record.last_update
                    }
                    return json.dumps(info, cls=CJsonEncoder)
                elif record.paused == 1:
                    info = {
                        "success": True,
                        "state": "paused",
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


class PipelineLog(object):

    @check_sig
    def GET(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "id") or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if client == record.client:
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


class PipelineStop(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "id") and hasattr(data, "reason")) or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if client == record.client:
                if record.has_run == 1 and (record.is_end or record.is_error):
                    info = {"success": False, "info": "流程已经结束！"}
                    return json.dumps(info)
                else:
                    insert_data = {"client": client,
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


class PipelineRunning(object):

    @check_sig
    def GET(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        workflow_module = Workflow()
        data = workflow_module.get_running(client)
        count = len(data)
        if count > 0:
            info = {}
            for rec in data:
                info["id"] = rec.workflow_id
                info["addtime"] = rec.add_time
                info["runtime"] = rec.run_time
            info = {"success": True, "count": count, "info": info}
            return json.dumps(info)
        else:
            info = {"success": True, "count": 0, "info": "没有正在运行的流程！"}
            return json.dumps(info)


class PipelineQueue(object):
    @check_sig
    def GET(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        workflow_module = Workflow()
        data = workflow_module.get_queue(client)
        count = len(data)
        if count > 0:
            info = {}
            for rec in data:
                info["id"] = rec.workflow_id
                info["addtime"] = rec.add_time
            info = {"success": True, "count": count, "info": info}
            return json.dumps(info)
        else:
            info = {"success": True, "count": 0, "info": "没有正在排队的流程！"}
            return json.dumps(info)


class PipelinePause(object):

    @check_sig
    def POST(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not (hasattr(data, "id") and hasattr(data, "reason")) or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if client == record.client:
                if record.has_run == 1 and (record.is_end or record.is_error):
                    info = {"success": False, "info": "流程已经结束！"}
                    return json.dumps(info)
                elif record.paused == 1:
                    info = {"success": False, "info": "流程已经暂停，请先结束暂停！"}
                    return json.dumps(info)
                else:
                    insert_data = {"client": client,
                                   "ip": web.ctx.ip,
                                   "reason": data.reason
                                   }
                    if workflow_module.set_pause(data.id, insert_data):
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


class PipelineStopPause(object):

    def POST(self):
        return self.GET()

    @check_sig
    def GET(self):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if not hasattr(data, "id") or data.id.strip() == "":
            info = {"success": False, "info": "缺少参数!"}
            return json.dumps(info)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(data.id)
        if len(workflow_data) > 0:
            record = workflow_data[0]
            if client == record.client:
                if record.has_run == 1 and (record.is_end or record.is_error):
                    info = {"success": False, "info": "流程已经结束！"}
                    return json.dumps(info)
                elif record.paused != 1:
                    info = {"success": False, "info": "流程未暂停,不能退出暂停！"}
                    return json.dumps(info)
                else:
                    if workflow_module.exit_pause(data.id):
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
