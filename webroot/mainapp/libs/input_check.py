# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import functools
import json
import web
from xml.etree.ElementTree import ParseError
import xml.etree.ElementTree as ET
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.distance_matrix import Distance


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


def MetaCheck(f):
    @functools.wraps(f)
    def wrapper(obj):
        data = web.input()
        print "收到请求, 请求的内容为："
        print data
        if not hasattr(data, "taskType"):
            print "缺少参数taskType"
            info = {"success": False, "info": "缺少参数taskType!"}
            return json.dumps(info)
        if data.taskType not in ["projectTask", "reportTask"]:
            print "参数taskType的值必须为projectTask或者是reportTask!"
            info = {"success": False, "info": "参数taskType的值必须为projectTask或者是reportTask!"}
            return json.dumps(info)
        if not hasattr(data, "otu_id"):
            if not hasattr(data, 'specimen_distance_id'):
                print "缺少otu_id或者specimen_distance_id(聚类独有)!"
                info = {"success": False, "info": "缺少otu_id或者在聚类分析中缺少距离矩阵specimen_distance_id!"}
                return json.dumps(info)
            else:
                table_info = Distance().get_distance_matrix_info(distance_id=data.specimen_distance_id)
                if not table_info:
                    print "specimen_distance_id不存在，请确认参数是否正确！!"
                    info = {"success": False, "info": "specimen_distance_id不存在，请确认参数是否正确！!"}
                    return json.dumps(info)
        else:
            table_info = Meta().get_otu_table_info(data.otu_id)
            if not table_info:
                print "OTU不存在，请确认参数是否正确！!"
                info = {"success": False, "info": "OTU不存在，请确认参数是否正确！!"}
                return json.dumps(info)
        task_info = Meta().get_task_info(table_info["task_id"])
        if not task_info:
            print "这个otu表对应的task：{}没有member_id!".format(table_info["task_id"])
            info = {"success": False, "info": "这个otu表对应的task：{}没有member_id!".format(table_info["task_id"])}
            return json.dumps(info)
        print "MetaCheck完成"
        return f(obj)

    return wrapper
