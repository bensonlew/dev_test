# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig, CreateSignature
from web import form
from mainapp.libs.json_check import check_json
import json
from mainapp.models.workflow import Workflow


class Pipline(object):

    def GET(self):
        render = web.template.render('mainapp/views')
        return render.pipline(self.get_form())

    @check_sig
    @check_json
    def POST(self):
        data = web.input()
        json_obj = json.loads(data.json)
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(json_obj['id'])
        if len(workflow_data) > 0:
            print workflow_data[0]
            info = {"success": False, "info": u"流程ID重复!"}
            return json.dumps(info)
        else:
            insert_data = {"client": data.client,
                           "workflow_id": json_obj['id'],
                           "json": data.json,
                           "ip": web.ctx.ip
                           }
            workflow_module.add_record(insert_data)
            info = {"success": True, "info": u"添加队列成功!"}
            return json.dumps(info)

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

