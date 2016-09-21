# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
from mainapp.libs.signature import check_sig
from mbio.packages.meta.copy_demo import CopyMongo


class DemoMongodataCopy(object):
    @check_sig
    def POST(self):
        data = web.input()
        # client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        requires = ['task_id', 'target_task_id', 'target_project_sn', 'target_member_id']
        for i in requires:
            if not (hasattr(data, i)):
                return json.dumps({"success": False, "info": "缺少%s参数!" % i})
        copy_task = CopyMongo(data.task_id, data.target_task_id, data.target_project_sn, data.target_member_id)
        try:
            copy_task.run()
        except Exception as e:
            print e
            return json.dumps({"success": False, "info": "拉取复制失败"})
        return json.dumps({"success": True, "info": "数据复制成功"})
