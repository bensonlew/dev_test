# -*- coding: utf-8 -*-
# __author__ = 'hesheng'
import web
import json
from mainapp.libs.signature import check_sig
from biocluster.config import Config
from mbio.packages.meta.copy_demo import CopyMongo
import traceback


class DemoMongodataCopy(object):
    @check_sig
    def POST(self):
        data = web.input()
        config = Config()
        if config.MONGODB == 'tsanger':
            db = 'tsanger'
        elif config.MONGODB == 'sanger':
            db = 'sanger'
        else:
            return json.dumps({"success": False, "info": "client不正确或者没有权限"})
        requires = ['task_id', 'target_task_id', 'target_project_sn', 'target_member_id']
        for i in requires:
            if not (hasattr(data, i)):
                return json.dumps({"success": False, "info": "缺少%s参数!" % i})
        copy_task = CopyMongo(data.task_id, data.target_task_id, data.target_project_sn, data.target_member_id, db=db)
        try:
            copy_task.run()
        except Exception:
            print traceback.format_exc()
            return json.dumps({"success": False, "info": "复制Demo样例数据失败"})
        return json.dumps({"success": True, "info": "数据复制成功"})
