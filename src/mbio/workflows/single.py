
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.workflow import Workflow
from biocluster.core.function import get_clsname_form_path
import importlib
import traceback


class SingleWorkflow(Workflow):
    """
    单独运行单个Module或Tool时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self._task = None
        super(SingleWorkflow, self).__init__(wsheet_object)

    def run(self):
        if self._sheet.type == "module":
            self._task = self.add_module(self._sheet.name)
        if self._sheet.type == "tool":
            self._task = self.add_tool(self._sheet.name)
        if self.UPDATE_STATUS_API:
            self._task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        if self.IMPORT_REPORT_DATA is True:
            self._task.IMPORT_REPORT_DATA = True
        if self.IMPORT_REPORT_AFTER_END is True:
            self._task.IMPORT_REPORT_AFTER_END = True
        self._task.sheet = self._sheet
        self._task.set_options(self._sheet.options())
        self._task.on('end', self.end)
        self._task.run()
        self.output_dir = self._task.output_dir
        super(SingleWorkflow, self).run()

    def end(self):
        self._upload_dir_obj = self._task.upload_dir
        self.run_mongo()
        super(SingleWorkflow, self).end()

    def run_mongo(self):
        """
        """
        class_name = get_clsname_form_path(self._sheet.name, tp='')
        dir_name = {
            "Agent": "mbio.api.database.toolapps.tools.",
            "Tool": "mbio.api.database.toolapps.tools.",
            "Module": "mbio.api.database.toolapps.modules.",
            "Workflow": "mbio.api.database.toolapps.workflows."
        }
        mudule_name = dir_name[self._sheet.type.capitalize()] + self._sheet.name
        try:
            imp = importlib.import_module(mudule_name)
            mongo_class = getattr(imp, class_name)
            task_mongo = mongo_class(self)
            task_mongo.manager = self.api
            task_mongo.run()
            pass
        except Exception:
            print traceback.format_exc()
            return
