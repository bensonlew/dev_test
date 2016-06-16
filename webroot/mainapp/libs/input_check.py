# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import functools
import json
import web
from xml.etree.ElementTree import ParseError
import xml.etree.ElementTree as ET
from mainapp.models.mongo.meta import Meta


def check_format(f):
    @functools.wraps(f)
    def wrapper(obj):
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == "client01" or client == "client03":
            if not hasattr(data, "content"):
                info = {"success": False, "info": "缺少参数: content！"}
                return json.dumps(info)
            xml_data = "".join(data.content)
            try:
                root = ET.fromstring(xml_data)
            except ParseError, e:
                info = {"success": False, "info": "xml格式不正确:%s" % e}
                return json.dumps(info)
            else:
                if root.tag != "workflow":
                    info = {"success": False, "info": "xml根节点必须为workflow!"}
                    return json.dumps(info)
                if root.find("project_sn") is None or root.find("project_sn").text == "":
                    info = {"success": False, "info": "xml中必须含有project_sn有效值!"}
                    return json.dumps(info)
                if root.find("task_id") is None or root.find("task_id").text == "":
                    info = {"success": False, "info": "xml中必须含有task_id有效值!"}
                    return json.dumps(info)
                if root.find("bucket") is None or root.find("task_id").text == "":
                    info = {"success": False, "info": "xml中必须含有bucket有效值!"}
                    return json.dumps(info)
                return f(obj)
        else:
            if (not hasattr(data, "json")) or len(data.json) < 1:
                info = {"success": False, "info": "缺少参数: json！"}
                return json.dumps(info)
            try:
                json.loads(data.json)
                return f(obj)
            except ValueError:
                info = {"success": False, "info": "json格式不正确!"}
                return json.dumps(info)

    return wrapper


def instantCheck(f):
    @functools.wraps(f)
    def wrapper(obj):
        data = web.input()
        print "收到请求, 请求的内容为："
        print data
        if not hasattr(data, "taskType"):
            info = {"success": False, "info": "缺少参数taskType!"}
            return json.dumps(info)
        if not (hasattr(data, "taskId") or hasattr(data, "otu_id")):
            info = {"success": False, "info": "参数taskId和otu_id必须有一个!"}
            return json.dumps(info)
        if data.taskType not in ["projectTask", "reportTask"]:
            info = {"success": False, "info": "参数taskType的值必须为projectTask或者是reportTask!"}
            return json.dumps(info)
        if hasattr(data, "otu_id"):
            otu_info = Meta().get_otu_table_info(data.otu_id)
            if not otu_info:
                info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
                return json.dumps(info)
            else:
                task_info = Meta().get_task_info(otu_info["task_id"])
                if not task_info:
                    info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(otu_info["task_id"])}
                    return json.dumps(info)
        return f(obj)

    return wrapper
