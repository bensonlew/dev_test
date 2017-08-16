# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

""""""

from biocluster.workflow import Workflow


class RefrnaCopyDemoWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RefrnaCopyDemoWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "task_id", "type": 'string', "default": ''},
            {"name": "target_task_id", "type": 'string', "default": ''},
            {"name": "target_project_sn", "type": 'string', "default": ''},
            {"name": "target_member_id", "type": 'string', "default": ''}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def check(self):
        pass

    def run(self):
        self.start_listener()
        self.fire("start")
        from mbio.packages.rna.refrna_copy_demo import RefrnaCopyMongo
        copy_task = RefrnaCopyMongo(self.option("task_id"), self.option("target_task_id"), self.option("target_project_sn"), self.option("target_member_id"))
        copy_task.run()
        self.end()

    def end(self):
        super(RefrnaCopyDemoWorkflow, self).end()
