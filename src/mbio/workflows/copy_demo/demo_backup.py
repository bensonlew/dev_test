# -*- coding: utf-8 -*-
# __author__ = 'shijin'

"""
对有参RNA demo数据进行备份
"""

from biocluster.workflow import Workflow


class DemoBackupWorkflow(Workflow):
    """
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DemoBackupWorkflow, self).__init__(wsheet_object)
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
        new_bam_path = "/mnt/ilustre/users/sanger/test/bam/"
        new_ref_gtf = "/mnt/ilustre/users/sanger/test/Mus_musculus.GRCm38.87.gff3.gtf"
        copy_task = RefrnaCopyMongo(old_task_id=self.option("task_id"),
                                    new_task_id=self.option("target_task_id"),
                                    new_project_sn=self.option("target_project_sn"),
                                    new_member_id=self.option("target_member_id"),
                                    new_bam_path=new_bam_path,
                                    new_ref_gtf=new_ref_gtf,
                                    db=self.config.MONGODB + "_ref_rna",
                                    )
        copy_task.run()
        self.end()

    def end(self):
        super(DemoBackupWorkflow, self).end()
