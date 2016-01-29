# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.workflow import Workflow


class SingleWorkflow(Workflow):
    """
    单独运行单个Module或Tool时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(SingleWorkflow, self).__init__(wsheet_object)

    def run(self):
        task = None
        if self._sheet.type == "module":
            task = self.add_module(self._sheet.name)
        if self._sheet.type == "tool":
            task = self.add_tool(self._sheet.name)
        if self.UPDATE_STATUS_API:
            task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        task.set_options(self._sheet.options())
        task.on('end', self.end)
        task.run()
        self.output_dir = task.output_dir
        super(SingleWorkflow, self).run()
